package s2.flink.source.enumerator;

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
    return EnumeratorState.newBuilder()
        .setInitialDistributionCompleted(obj.initialDistributionCompleted)
        .addAllStreams(obj.streams)
        .addAllUnassignedStreams(
            obj.unassignedStreams.stream()
                .map(S2SplitSerializer::intoProto)
                .collect(Collectors.toList()))
        .build()
        .toByteArray();
  }

  @Override
  public S2EnumeratorState deserialize(int version, byte[] serialized) throws IOException {
    if (version != VERSION) {
      throw new IOException("Unknown version: " + version);
    }
    var enumeratorState = EnumeratorState.parseFrom(serialized);
    return new S2EnumeratorState(
        enumeratorState.getStreamsList(),
        enumeratorState.getUnassignedStreamsList().stream()
            .map(S2SplitSerializer::fromProto)
            .collect(Collectors.toList()),
        enumeratorState.getInitialDistributionCompleted());
  }
}
