package s2.flink.sink;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.apache.flink.connector.base.sink.writer.AsyncSinkWriterStateSerializer;
import s2.types.AppendRecord;
import s2.types.Header;

public class S2StateSerializer extends AsyncSinkWriterStateSerializer<AppendRecord> {

  @Override
  protected void serializeRequestToStream(AppendRecord request, DataOutputStream out)
      throws IOException {
    s2.v1alpha.AppendRecord data = request.toProto();
    out.write(data.toByteArray());
  }

  @Override
  protected AppendRecord deserializeRequestFromStream(long requestSize, DataInputStream in)
      throws IOException {
    s2.v1alpha.AppendRecord appendRecord = s2.v1alpha.AppendRecord.parseFrom(in.readAllBytes());
    return AppendRecord.newBuilder()
        .withBody(appendRecord.getBody())
        .withHeaders(appendRecord.getHeadersList().stream().map(Header::fromProto).toList())
        .build();
  }

  @Override
  public int getVersion() {
    return 0;
  }
}
