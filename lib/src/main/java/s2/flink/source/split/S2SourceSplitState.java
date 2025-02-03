package s2.flink.source.split;

import java.util.Optional;
import s2.types.SequencedRecord;

public class S2SourceSplitState {

  public final S2SourceSplit originalSplit;
  private long startSeqNum = 0;
  private long consumedRecords;
  private long consumedMeteredBytes;

  public S2SourceSplitState(S2SourceSplit split) {
    this.originalSplit = split;
    split.startSeqNum.ifPresent(
        startSeqNum -> {
          this.startSeqNum = startSeqNum;
        });
    this.consumedRecords = split.consumedRecords;
    this.consumedMeteredBytes = split.consumedMeteredBytes;
  }

  public void register(SequencedRecord record) {
    this.startSeqNum = record.seqNum() + 1;
    this.consumedRecords += 1;
    this.consumedMeteredBytes += record.meteredBytes();
  }

  public S2SourceSplit toSplit() {
    return new S2SourceSplit(
        originalSplit.splitId,
        originalSplit.startBehavior,
        this.consumedRecords > 0 ? Optional.of(startSeqNum) : Optional.empty(),
        this.consumedRecords,
        this.consumedMeteredBytes);
  }
}
