package s2.flink.record;

import com.google.protobuf.ByteString;
import java.util.List;
import java.util.Optional;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.RowKind;
import s2.types.Header;

public class Upsert {
  public static final ByteString FLAG_UPDATE = ByteString.copyFromUtf8("u");
  public static final ByteString FLAG_DELETE = ByteString.copyFromUtf8("d");

  public static final ByteString ACTION_HEADER_NAME = ByteString.copyFromUtf8("@action");
  public static final ByteString KEY_HEADER_NAME = ByteString.copyFromUtf8("@key");

  public static final Header ACTION_UPDATE_HEADER = new Header(ACTION_HEADER_NAME, FLAG_UPDATE);
  public static final Header ACTION_DELETE_HEADER = new Header(ACTION_HEADER_NAME, FLAG_DELETE);

  public static List<Header> keyUpdate(ByteString serializedKey) {
    return List.of(new Header(KEY_HEADER_NAME, serializedKey), ACTION_UPDATE_HEADER);
  }

  public static List<Header> keyDelete(ByteString serializedKey) {
    return List.of(new Header(KEY_HEADER_NAME, serializedKey), ACTION_DELETE_HEADER);
  }

  public static Optional<Tuple2<ByteString, RowKind>> extractKeyAndAction(List<Header> headers) {
    ByteString key = null;
    RowKind kind = null;
    for (var header : headers) {
      if (header.name.equals(KEY_HEADER_NAME)) {
        key = header.value;
      } else if (header.name.equals(ACTION_HEADER_NAME)) {
        if (header.value.equals(FLAG_UPDATE)) {
          kind = RowKind.UPDATE_AFTER;
        } else if (header.value.equals(FLAG_DELETE)) {
          kind = RowKind.DELETE;
        }
      }
      if (key != null && kind != null) {
        return Optional.of(new Tuple2<>(key, kind));
      }
    }
    return Optional.empty();
  }
}
