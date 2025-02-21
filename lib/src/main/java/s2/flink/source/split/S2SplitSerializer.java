package s2.flink.source.split;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Optional;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import s2.flink.internal.SourceSplit;
import s2.flink.internal.SourceSplit.StartBehavior;

public class S2SplitSerializer implements SimpleVersionedSerializer<S2SourceSplit> {

  private static final int VERSION = 1;

  @Override
  public int getVersion() {
    return VERSION;
  }

  @Override
  public byte[] serialize(S2SourceSplit obj) throws IOException {
    try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final DataOutputStream out = new DataOutputStream(baos)) {
      out.writeInt(this.getVersion());
      intoProto(obj).writeTo(out);
      return baos.toByteArray();
    }
  }

  public static SourceSplit intoProto(S2SourceSplit obj) {
    final var builder =
        SourceSplit.newBuilder()
            .setSplitId(obj.splitId)
            .setStartBehavior(StartBehavior.valueOf(obj.startBehavior.name()))
            .setConsumedRecords(obj.consumedRecords)
            .setConsumedMeteredBytes(obj.consumedMeteredBytes);
    obj.startSeqNum.ifPresent(builder::setStartSeqNum);
    return builder.build();
  }

  @Override
  public S2SourceSplit deserialize(int version, byte[] serialized) throws IOException {

    try (final ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
        final DataInputStream in = new DataInputStream(bais)) {
      final int versionNumber = in.readInt();
      if (versionNumber != this.getVersion()) {
        throw new IllegalStateException(
            "Serialized version mismatch. Expected: "
                + this.getVersion()
                + ", got: "
                + versionNumber);
      }

      return fromProto(SourceSplit.parseFrom(in));
    }
  }

  public static S2SourceSplit fromProto(SourceSplit sourceSplit) {
    final SplitStartBehavior startBehavior;
    switch (sourceSplit.getStartBehavior()) {
      case FIRST:
        startBehavior = SplitStartBehavior.FIRST;
        break;
      case NEXT:
        startBehavior = SplitStartBehavior.NEXT;
        break;
      default:
        throw new UnsupportedOperationException(
            "Unrecognized start behavior: " + sourceSplit.getStartBehavior());
    }
    final Optional<Long> startSeqNum =
        sourceSplit.hasStartSeqNum() ? Optional.of(sourceSplit.getStartSeqNum()) : Optional.empty();
    return new S2SourceSplit(
        sourceSplit.getSplitId(),
        startBehavior,
        startSeqNum,
        sourceSplit.getConsumedRecords(),
        sourceSplit.getConsumedMeteredBytes());
  }
}
