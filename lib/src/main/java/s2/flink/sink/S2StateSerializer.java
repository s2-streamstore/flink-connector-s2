package s2.flink.sink;

import com.google.protobuf.InvalidProtocolBufferException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.stream.Collectors;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.RequestEntryWrapper;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import s2.flink.internal.BufferedRequestState.WrappedRequest;
import s2.types.AppendRecord;
import s2.types.Header;

public class S2StateSerializer
    implements SimpleVersionedSerializer<BufferedRequestState<AppendRecord>> {

  private static final int VERSION = 1;

  @Override
  public int getVersion() {
    return VERSION;
  }

  @Override
  public byte[] serialize(BufferedRequestState<AppendRecord> obj) throws IOException {

    try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final DataOutputStream out = new DataOutputStream(baos)) {
      out.writeInt(this.getVersion());
      s2.flink.internal.BufferedRequestState.newBuilder()
          .addAllEntries(
              obj.getBufferedRequestEntries().stream()
                  .map(
                      w ->
                          WrappedRequest.newBuilder()
                              .setSize(w.getSize())
                              .setEntry(w.getRequestEntry().toProto().toByteString())
                              .build())
                  .collect(Collectors.toList()))
          .build()
          .writeTo(out);
      return baos.toByteArray();
    }
  }

  @Override
  public BufferedRequestState<AppendRecord> deserialize(int version, byte[] serialized)
      throws IOException {
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
      return new BufferedRequestState<>(
          s2.flink.internal.BufferedRequestState.parseFrom(in).getEntriesList().stream()
              .map(
                  w -> {
                    try {
                      var appendRecord = s2.v1alpha.AppendRecord.parseFrom(w.getEntry());
                      return new RequestEntryWrapper<>(
                          AppendRecord.newBuilder()
                              .withBody(appendRecord.getBody())
                              .withHeaders(
                                  appendRecord.getHeadersList().stream()
                                      .map(Header::fromProto)
                                      .collect(Collectors.toList()))
                              .build(),
                          w.getSize());
                    } catch (InvalidProtocolBufferException e) {
                      throw new RuntimeException(e);
                    }
                  })
              .collect(Collectors.toList()));
    }
  }
}
