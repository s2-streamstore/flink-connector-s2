package s2.flink.source.reader;

import static s2.flink.config.S2SourceConfig.S2_PER_SPLIT_READ_SESSION_BUFFER_BYTES;
import static s2.flink.config.S2SourceConfig.S2_SOURCE_BASIN;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import java.io.IOException;
import java.rmi.UnexpectedException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import s2.channel.BasinChannel;
import s2.channel.ManagedChannelFactory;
import s2.client.ManagedReadSession;
import s2.client.StreamClient;
import s2.config.Config;
import s2.flink.config.S2ClientConfig;
import s2.flink.source.split.S2SourceSplit;
import s2.types.Batch;
import s2.types.FirstSeqNum;
import s2.types.NextSeqNum;
import s2.types.ReadLimit;
import s2.types.ReadOutput;
import s2.types.ReadRequest;
import s2.types.ReadSessionRequest;
import s2.types.SequencedRecord;

public class S2SplitReader implements SplitReader<SequencedRecord, S2SourceSplit> {

  private static final Logger LOG = LoggerFactory.getLogger(S2SplitReader.class);
  private final ConcurrentLinkedDeque<Tuple2<Long, S2SourceSplit>> splitsWithStart =
      new ConcurrentLinkedDeque<>();
  private final List<Tuple2<S2SourceSplit, ManagedReadSession>> splitSessions = new ArrayList<>();
  private final Config s2Config;
  private final String basinName;
  private final BasinChannel basinChannel;
  private final Configuration sourceConfig;
  private final ScheduledThreadPoolExecutor executor;
  private final AtomicReference<Throwable> cachedError = new AtomicReference<>();
  private int splitReaderIndex = 0;

  public S2SplitReader(Configuration sourceConfig) {
    LOG.debug("split reader start");
    this.sourceConfig = sourceConfig;
    this.executor = new ScheduledThreadPoolExecutor(1);
    this.s2Config = S2ClientConfig.fromConfig(sourceConfig);
    this.basinName = sourceConfig.get(S2_SOURCE_BASIN);
    this.basinChannel =
        ManagedChannelFactory.forBasinOrStreamService(this.s2Config, this.basinName);
  }

  @Override
  public RecordsWithSplitIds<SequencedRecord> fetch() throws IOException {
    // Start any pending sessions.
    initializeSessions();

    if (cachedError.get() != null) {
      throw new IOException(cachedError.get());
    }

    if (splitSessions.isEmpty()) {
      return new RecordsBySplits<>(Map.of(), Set.of());
    }

    final var reader = splitSessions.get(splitReaderIndex % splitSessions.size());
    splitReaderIndex++;

    final var split = reader.f0;
    final var session = reader.f1;

    if (session.isClosed()) {
      LOG.error("session closed for split={}", split.splitId());
      throw new UnexpectedException(
          "Read session closed without error, for split=" + split.splitId());
    }
    return session
        .get()
        .map(
            resp -> {
              if (resp instanceof Batch batch) {
                return new RecordsBySplits<>(
                    Map.of(split.splitId(), batch.sequencedRecordBatch().records()), Set.of());
              } else if (resp instanceof FirstSeqNum firstSeqNum) {
                throw new RuntimeException("Should retry: firstSeqNum=" + firstSeqNum);
              } else if (resp instanceof NextSeqNum nextSeqNum) {
                throw new RuntimeException("Unexpected batch response.");
              }
              throw new RuntimeException("Unrecognized response.");
            })
        .orElse(new RecordsBySplits<>(Map.of(), Set.of()));
  }

  private void initializeSessions() {
    var sessionsToInit = this.splitsWithStart.size();
    for (int i = 0; i < sessionsToInit; i++) {
      var splitWithStart = splitsWithStart.poll();
      var readSession =
          StreamClient.newBuilder(this.s2Config, this.basinName, splitWithStart.f1.splitId())
              .build()
              .managedReadSession(
                  ReadSessionRequest.newBuilder().withStartSeqNum(splitWithStart.f0).build(),
                  this.sourceConfig.get(S2_PER_SPLIT_READ_SESSION_BUFFER_BYTES));

      splitSessions.add(Tuple2.of(splitWithStart.f1, readSession));
    }
  }

  @Override
  public void handleSplitsChanges(SplitsChange<S2SourceSplit> splitsChanges) {
    LOG.debug("handleSplitsChanges: {}", splitsChanges);
    if (splitsChanges instanceof SplitsAddition<S2SourceSplit> addition) {
      addition
          .splits()
          .forEach(
              split -> {
                if (split.startSeqNum().isPresent()) {
                  // Resuming from a recovered split.
                  splitsWithStart.push(Tuple2.of(split.startSeqNum().get(), split));
                } else {
                  // New split. Need to determine where to start our session, contingent on the
                  // split start behavior.
                  switch (split.startBehavior()) {
                    case FIRST ->
                        Futures.addCallback(
                            StreamClient.newBuilder(this.s2Config, this.basinName, split.splitId())
                                .withChannel(this.basinChannel)
                                .withExecutor(this.executor)
                                .build()
                                .read(
                                    ReadRequest.newBuilder()
                                        .withStartSeqNum(0)
                                        .withReadLimit(ReadLimit.count(1))
                                        .build()),
                            new FutureCallback<ReadOutput>() {
                              @Override
                              public void onSuccess(ReadOutput result) {
                                if (result instanceof Batch batch) {
                                  // No trimming. Start at 0.
                                  splitsWithStart.push(Tuple2.of(0L, split));
                                } else if (result instanceof FirstSeqNum firstSeqNum) {
                                  // Trimming has occurred.
                                  splitsWithStart.push(Tuple2.of(firstSeqNum.value(), split));

                                } else if (result instanceof NextSeqNum nextSeqNum) {
                                  cachedError.set(
                                      new RuntimeException("Unexpected nextSeqNum: " + nextSeqNum));
                                }
                              }

                              @Override
                              public void onFailure(Throwable t) {
                                cachedError.set(t);
                              }
                            },
                            this.executor);
                    case NEXT ->
                        Futures.addCallback(
                            StreamClient.newBuilder(this.s2Config, this.basinName, split.splitId())
                                .withChannel(this.basinChannel)
                                .withExecutor(this.executor)
                                .build()
                                .checkTail(),
                            new FutureCallback<Long>() {
                              @Override
                              public void onSuccess(Long result) {
                                splitsWithStart.push(Tuple2.of(result, split));
                              }

                              @Override
                              public void onFailure(Throwable t) {
                                cachedError.set(t);
                              }
                            },
                            this.executor);
                  }
                }
              });

    } else {
      throw new UnsupportedOperationException(
          "Unexpected receipt of splits change: " + splitsChanges);
    }
  }

  @Override
  public void wakeUp() {
    // Fetch should never block.
  }

  @Override
  public void close() throws Exception {
    this.splitSessions.stream()
        .forEach(
            session -> {
              try {
                session.f1.close();
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });
    this.basinChannel.close();
    this.executor.shutdown();
  }
}
