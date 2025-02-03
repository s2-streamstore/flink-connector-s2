package s2.flink.source.split;

import java.io.IOException;
import java.util.Optional;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import s2.flink.internal.SourceSplit;
import s2.flink.internal.SourceSplit.StartBehavior;

public class S2SplitSerializer implements SimpleVersionedSerializer<S2SourceSplit> {

  private static final int VERSION = 1;
  private static final Logger LOG = LoggerFactory.getLogger(S2SplitSerializer.class);

  @Override
  public int getVersion() {
    return VERSION;
  }

  @Override
  public byte[] serialize(S2SourceSplit obj) throws IOException {
    return intoProto(obj).toByteArray();
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
    LOG.debug("split deserialize");

    if (version != VERSION) {
      throw new IOException("Unsupported version: " + version);
    }
    return fromProto(SourceSplit.parseFrom(serialized));
  }

  public static S2SourceSplit fromProto(SourceSplit sourceSplit) {
    final SplitStartBehavior startBehavior =
        switch (sourceSplit.getStartBehavior()) {
          case FIRST -> SplitStartBehavior.FIRST;
          case NEXT -> SplitStartBehavior.NEXT;
          case UNRECOGNIZED ->
              throw new UnsupportedOperationException(
                  "Unrecognized start behavior: " + sourceSplit.getStartBehavior());
        };
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
