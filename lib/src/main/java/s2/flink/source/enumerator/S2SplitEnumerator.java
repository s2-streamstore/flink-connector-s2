package s2.flink.source.enumerator;

import static s2.flink.config.S2SourceConfig.S2_SOURCE_SPLIT_START_BEHAVIOR;
import static s2.flink.config.S2SourceConfig.S2_SOURCE_STREAMS;
import static s2.flink.config.S2SourceConfig.S2_SOURCE_STREAM_DISCOVERY_CADENCE_MS;
import static s2.flink.config.S2SourceConfig.S2_SOURCE_STREAM_DISCOVERY_PREFIX;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import s2.flink.source.split.S2SourceSplit;
import s2.flink.source.split.SplitStartBehavior;

public abstract class S2SplitEnumerator
    implements SplitEnumerator<S2SourceSplit, S2EnumeratorState> {

  private static final Logger LOG = LoggerFactory.getLogger(S2SplitEnumerator.class);
  final SplitEnumeratorContext<S2SourceSplit> enumeratorContext;
  final Configuration sourceConfig;
  final SplitStartBehavior startBehavior;

  // The universe of known S2 streams for this source.
  // This is either set statically, from streams specified by the user, or discovered based
  // on a provided search prefix (and potentially updated on a configured cadence).
  final ConcurrentSkipListSet<String> discoveredStreams = new ConcurrentSkipListSet<>();

  // The subset of `discoveredStreams` which have yet to be assigned to a reader.
  final ConcurrentLinkedQueue<S2SourceSplit> unassignedSplits = new ConcurrentLinkedQueue<>();

  final AtomicBoolean initialDistributionCompleted = new AtomicBoolean(false);

  public S2SplitEnumerator(
      SplitEnumeratorContext<S2SourceSplit> splitEnumeratorContext,
      Configuration sourceConfiguration) {
    this.enumeratorContext = splitEnumeratorContext;
    this.sourceConfig = sourceConfiguration;
    this.startBehavior = sourceConfiguration.get(S2_SOURCE_SPLIT_START_BEHAVIOR);
  }

  public static S2SplitEnumerator construct(
      SplitEnumeratorContext<S2SourceSplit> enumContext, Configuration sourceConfig) {
    // Determine if we need a static, or dynamic enumerator.
    final var specifiedStreams = sourceConfig.getOptional(S2_SOURCE_STREAMS);

    if (specifiedStreams.isPresent()) {
      return new S2StaticSplitEnumerator(enumContext, sourceConfig, specifiedStreams.get());
    }

    final var prefix = sourceConfig.getOptional(S2_SOURCE_STREAM_DISCOVERY_PREFIX);
    if (prefix.isEmpty()) {
      throw new RuntimeException("S2 source had no specified streams, or prefix for discovery.");
    }
    final Optional<Duration> refreshCadence =
        sourceConfig.getOptional(S2_SOURCE_STREAM_DISCOVERY_CADENCE_MS).map(Duration::ofMillis);
    return new S2DynamicSplitEnumerator(
        enumContext, sourceConfig, prefix.get(), refreshCadence, List.of(), List.of(), false);
  }

  public static S2SplitEnumerator restoreFromSnapshot(
      SplitEnumeratorContext<S2SourceSplit> enumContext,
      Configuration sourceConfig,
      List<String> discoveredStreams,
      List<S2SourceSplit> unassignedStreams,
      boolean initialDistributionComplete) {
    if (sourceConfig.getOptional(S2_SOURCE_STREAMS).isPresent()) {
      return new S2StaticSplitEnumerator(
          enumContext,
          sourceConfig,
          discoveredStreams,
          unassignedStreams,
          initialDistributionComplete);
    } else {
      final var prefix = sourceConfig.getOptional(S2_SOURCE_STREAM_DISCOVERY_PREFIX);
      if (prefix.isEmpty()) {
        throw new RuntimeException("S2 source had no specified streams, or prefix for discovery.");
      }
      final Optional<Duration> refreshCadence =
          sourceConfig.getOptional(S2_SOURCE_STREAM_DISCOVERY_CADENCE_MS).map(Duration::ofMillis);
      return new S2DynamicSplitEnumerator(
          enumContext,
          sourceConfig,
          prefix.get(),
          refreshCadence,
          discoveredStreams,
          unassignedStreams,
          initialDistributionComplete);
    }
  }

  @Override
  public void start() {}

  @Override
  public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
    // if there are unassigned, assign it to the reader, update tracker
    // this.enumeratorContext.assignSplits();
    System.out.println("handleSplitRequest");
    // enumeratorContext.sendEventToSourceReader();
  }

  @Override
  public void addSplitsBack(List<S2SourceSplit> splits, int subtaskId) {
    LOG.debug("addSplitsBack, subtaskId: {}, splits: {}", subtaskId, splits);
    if (!splits.isEmpty()) {
      // A source reader failed and assignments since it was last snapshotted are being returned for
      // reassignment.
      // Instead of maintaining state for this situation, we propagate the failure to the
      // enumerator.
      throw new UnsupportedOperationException(
          "Adding splits back. Enumerator should go down instead.");
    }
  }

  @Override
  public void addReader(int subtaskId) {
    if (enumeratorContext.registeredReaders().size() == enumeratorContext.currentParallelism()
        && !unassignedSplits.isEmpty()
        && !initialDistributionCompleted.get()) {
      distributeInitialSplits();
    }

    // Any new reader added since the original distribution will only be eligible for splits
    // assigned by the dynamic enumerator, discovered after the reader is added. That is done
    // by `S2DynamicSplitEnumerator::processStreams`.
  }

  abstract void distributeInitialSplits();

  @Override
  public S2EnumeratorState snapshotState(long checkpointId) throws Exception {
    LOG.debug("snapshotState, checkpointId={}", checkpointId);
    return new S2EnumeratorState(
        new ArrayList<>(discoveredStreams),
        new ArrayList<>(unassignedSplits),
        this.initialDistributionCompleted.get());
  }

  @Override
  public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
    LOG.debug("handleSourceEvent, subtaskId={}", subtaskId);
  }
}
