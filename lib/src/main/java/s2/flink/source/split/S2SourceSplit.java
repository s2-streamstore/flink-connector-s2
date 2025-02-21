package s2.flink.source.split;

import java.util.Optional;
import org.apache.flink.api.connector.source.SourceSplit;

public class S2SourceSplit implements SourceSplit {

  final String splitId;
  final SplitStartBehavior startBehavior;
  final Optional<Long> startSeqNum;
  final long consumedRecords;
  final long consumedMeteredBytes;

  // realistically this is probably:
  // - streamName
  // - startSeqNum (behavior)

  public S2SourceSplit(String splitId, SplitStartBehavior startBehavior) {
    this(splitId, startBehavior, Optional.empty(), 0, 0);
  }

  public S2SourceSplit(
      String splitId,
      SplitStartBehavior startBehavior,
      Optional<Long> startSeqNum,
      long consumedRecords,
      long consumedMeteredBytes) {
    this.splitId = splitId;
    this.startBehavior = startBehavior;
    this.startSeqNum = startSeqNum;
    this.consumedRecords = consumedRecords;
    this.consumedMeteredBytes = consumedMeteredBytes;
  }

  public Optional<Long> startSeqNum() {
    return startSeqNum;
  }

  public SplitStartBehavior startBehavior() {
    return startBehavior;
  }

  @Override
  public String splitId() {
    return this.splitId;
  }
}
