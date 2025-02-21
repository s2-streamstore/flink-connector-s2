package s2.flink.source.enumerator;

import java.util.List;
import s2.flink.source.split.S2SourceSplit;

public final class S2EnumeratorState {

  public final List<String> streams;
  public final List<S2SourceSplit> unassignedStreams;
  public final boolean initialDistributionCompleted;

  public S2EnumeratorState(
      List<String> streams,
      List<S2SourceSplit> unassignedStreams,
      boolean initialDistributionCompleted) {
    this.streams = streams;
    this.unassignedStreams = unassignedStreams;
    this.initialDistributionCompleted = initialDistributionCompleted;
  }
}
