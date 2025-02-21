package s2.flink.source.enumerator;

import java.io.IOException;
import java.util.List;
import java.util.stream.IntStream;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import s2.flink.source.split.S2SourceSplit;

public class S2StaticSplitEnumerator extends S2SplitEnumerator {

  private static final Logger LOG = LoggerFactory.getLogger(S2StaticSplitEnumerator.class);

  public S2StaticSplitEnumerator(
      SplitEnumeratorContext<S2SourceSplit> splitEnumeratorContext,
      Configuration sourceConfiguration,
      List<String> streams) {
    super(splitEnumeratorContext, sourceConfiguration);
    this.discoveredStreams.addAll(streams);
    streams.stream()
        .map(stream -> new S2SourceSplit(stream, this.startBehavior))
        .forEach(unassignedSplits::add);
  }

  public S2StaticSplitEnumerator(
      SplitEnumeratorContext<S2SourceSplit> splitEnumeratorContext,
      Configuration sourceConfiguration,
      List<String> streams,
      List<S2SourceSplit> unassignedStreams,
      boolean initialDistributionCompleted) {
    super(splitEnumeratorContext, sourceConfiguration);
    this.discoveredStreams.addAll(streams);
    this.unassignedSplits.addAll(unassignedStreams);
    this.initialDistributionCompleted.set(initialDistributionCompleted);
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
              if (splitsToRead == 0) {
                this.enumeratorContext.signalNoMoreSplits(subtask);
              } else {
                for (int i = 0; i < splitsToRead; i++) {
                  this.enumeratorContext.assignSplit(this.unassignedSplits.poll(), subtask);
                }
              }
            });

    this.initialDistributionCompleted.set(true);
  }

  @Override
  public void close() throws IOException {}
}
