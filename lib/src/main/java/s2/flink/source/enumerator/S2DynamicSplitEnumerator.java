package s2.flink.source.enumerator;

import static s2.flink.config.S2SourceConfig.S2_SOURCE_BASIN;

import com.google.common.collect.Streams;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import s2.channel.BasinChannel;
import s2.channel.ManagedChannelFactory;
import s2.client.BasinClient;
import s2.config.Config;
import s2.flink.config.S2ClientConfig;
import s2.flink.source.split.S2SourceSplit;
import s2.types.ListStreamsRequest;
import s2.types.StreamInfo;

public class S2DynamicSplitEnumerator extends S2SplitEnumerator {

  private static final Logger LOG = LoggerFactory.getLogger(S2DynamicSplitEnumerator.class);

  private final ScheduledThreadPoolExecutor scheduledThreadPoolExecutor;
  private final BasinChannel basinChannel;
  private final BasinClient basinClient;
  private final String discoveryPrefix;
  private final Optional<Duration> discoveryInterval;

  public S2DynamicSplitEnumerator(
      SplitEnumeratorContext<S2SourceSplit> splitEnumeratorContext,
      Configuration sourceConfiguration,
      String prefix,
      Optional<Duration> refreshCadence,
      List<String> streams,
      List<S2SourceSplit> unassignedStreams,
      boolean initialDistributionCompleted) {
    super(splitEnumeratorContext, sourceConfiguration);

    final Config config = S2ClientConfig.fromConfig(sourceConfiguration);
    this.scheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(1);
    this.basinChannel =
        ManagedChannelFactory.forBasinOrStreamService(
            config,
            this.sourceConfig
                .getOptional(S2_SOURCE_BASIN)
                .orElseThrow(() -> new IllegalStateException("No basin provided")));
    this.discoveryPrefix = prefix;
    this.discoveryInterval = refreshCadence;
    this.basinClient =
        BasinClient.newBuilder(config, this.sourceConfig.get(S2_SOURCE_BASIN))
            .withChannel(this.basinChannel)
            .withExecutor(this.scheduledThreadPoolExecutor)
            .build();

    discoveredStreams.addAll(streams);
    unassignedSplits.addAll(unassignedStreams);
    this.initialDistributionCompleted.set(initialDistributionCompleted);
  }

  private s2.types.ListStreamsRequest updatedStartAfter(
      s2.types.ListStreamsRequest request, String newStartAfter) {
    final var builder =
        s2.types.ListStreamsRequest.newBuilder()
            .withPrefix(request.prefix)
            .withStartAfter(newStartAfter);
    request.limit.ifPresent(builder::withLimit);

    return builder.build();
  }

  private ListenableFuture<List<StreamInfo>> listStreamsRecursive(
      s2.types.ListStreamsRequest listStreamsRequest, List<StreamInfo> accumulator) {
    return Futures.transformAsync(
        basinClient.listStreams(listStreamsRequest),
        resp -> {
          accumulator.addAll(resp.elems);
          if (resp.hasMore) {
            return listStreamsRecursive(
                updatedStartAfter(listStreamsRequest, resp.elems.get(resp.elems.size() - 1).name),
                accumulator);
          } else {
            return Futures.immediateFuture(accumulator);
          }
        },
        this.scheduledThreadPoolExecutor);
  }

  public ListenableFuture<List<StreamInfo>> listStreamsExhaustively(
      s2.types.ListStreamsRequest listStreamsRequest, Duration timeout) {
    return Futures.withTimeout(
        listStreamsRecursive(listStreamsRequest, new ArrayList<>()),
        timeout,
        scheduledThreadPoolExecutor);
  }

  @Override
  public void start() {
    LOG.debug("enumerator start");
    this.discoveryInterval.ifPresentOrElse(
        interval ->
            enumeratorContext.callAsync(
                this::getStreams, this::processStreams, 0, interval.toMillis()),
        () -> enumeratorContext.callAsync(this::getStreams, this::processStreams));
  }

  @Override
  void distributeInitialSplits() {
    final var numSplits = this.unassignedSplits.size();
    final var numReaders = this.enumeratorContext.registeredReaders().size();
    final var base = numSplits / numReaders;
    final var remainder = numSplits % numReaders;
    IntStream.range(0, numReaders)
        .forEach(
            subtask -> {
              final var splitsToRead = base + (subtask < remainder ? 1 : 0);
              LOG.debug("assigning {} for subtask {}", splitsToRead, subtask);
              if (splitsToRead == 0 && discoveryInterval.isEmpty()) {
                // No additional streams will be discovered, past this initial set.
                this.enumeratorContext.signalNoMoreSplits(subtask);
              } else {
                for (int i = 0; i < splitsToRead; i++) {
                  this.enumeratorContext.assignSplit(this.unassignedSplits.poll(), subtask);
                }
              }
            });

    this.initialDistributionCompleted.set(true);
  }

  private List<String> getStreams() {
    try {
      // TODO pagination
      var resp =
          this.listStreamsExhaustively(
                  ListStreamsRequest.newBuilder().withPrefix(discoveryPrefix).build(),
                  Duration.ofSeconds(10))
              .get()
              .stream()
              .filter(s -> s.deletedAt.isEmpty())
              .map(si -> si.name)
              .collect(Collectors.toList());
      LOG.debug(
          "found {} active matching streams, first={}",
          resp.size(),
          resp.stream().findFirst().orElse("none"));
      return resp;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void processStreams(List<String> updatedStreams, Throwable t) {
    if (updatedStreams == null || updatedStreams.isEmpty()) {
      LOG.warn("No matching streams found during refresh.");
      return;
    }

    updatedStreams.forEach(
        stream -> {
          if (discoveredStreams.add(stream)) {
            unassignedSplits.add(new S2SourceSplit(stream, this.startBehavior));
          }
        });

    final var currentReaders = enumeratorContext.registeredReaders();
    final var currentNumReaders = currentReaders.size();

    if (currentNumReaders == 0) {
      return;
    }

    if (currentNumReaders == enumeratorContext.currentParallelism()
        && !unassignedSplits.isEmpty()
        && !initialDistributionCompleted.get()) {
      distributeInitialSplits();
    }

    final List<S2SourceSplit> drained = new ArrayList<>();
    S2SourceSplit split;
    while ((split = unassignedSplits.poll()) != null) {
      drained.add(split);
    }
    Streams.mapWithIndex(
            drained.stream(),
            (unassigned, index) -> {
              var assignedReaderId = currentReaders.get((int) (index % currentNumReaders));
              return Tuple2.of(unassigned, assignedReaderId);
            })
        .forEach(pair -> enumeratorContext.assignSplit(pair.f0, pair.f1.getSubtaskId()));
  }

  @Override
  public void close() throws IOException {
    try {
      this.basinClient.close();
      this.basinChannel.close();
      this.scheduledThreadPoolExecutor.shutdown();
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
}
