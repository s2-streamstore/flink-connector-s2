package s2.flink.source.enumerator;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.stream.Collectors;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import s2.flink.internal.EnumeratorState;
import s2.flink.source.split.S2SplitSerializer;

public class S2EnumeratorStateSerializer implements SimpleVersionedSerializer<S2EnumeratorState> {

  public static final int VERSION = 1;

  @Override
  public int getVersion() {
    return VERSION;
  }

  @Override
  public byte[] serialize(S2EnumeratorState obj) throws IOException {
    try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final DataOutputStream out = new DataOutputStream(baos)) {

      out.writeInt(this.getVersion());
      EnumeratorState.newBuilder()
          .setInitialDistributionCompleted(obj.initialDistributionCompleted)
          .addAllStreams(obj.streams)
          .addAllUnassignedStreams(
              obj.unassignedStreams.stream()
                  .map(S2SplitSerializer::intoProto)
                  .collect(Collectors.toList()))
          .build()
          .writeTo(out);

      return baos.toByteArray();
    }
  }

  @Override
  public S2EnumeratorState deserialize(int version, byte[] serialized) throws IOException {

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

      var enumeratorState = EnumeratorState.parseFrom(in);
      return new S2EnumeratorState(
          enumeratorState.getStreamsList(),
          enumeratorState.getUnassignedStreamsList().stream()
              .map(S2SplitSerializer::fromProto)
              .collect(Collectors.toList()),
          enumeratorState.getInitialDistributionCompleted());
    }
  }
}
