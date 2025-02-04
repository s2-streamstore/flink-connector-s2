package s2.flink.sink.upsert;

import static s2.flink.sink.upsert.S2UpsertSinkRowDataConverter.KEY_NAME;

import com.google.common.collect.Iterables;
import com.google.common.collect.Streams;
import com.google.protobuf.ByteString;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.base.sink.writer.config.AsyncSinkWriterConfiguration;
import org.apache.flink.table.data.RowData;
import s2.flink.sink.S2SinkWriter;
import s2.types.AppendRecord;

class S2UpsertSinkWriter extends S2SinkWriter<RowData> {

  public S2UpsertSinkWriter(
      ElementConverter<RowData, AppendRecord> elementConverter,
      WriterInitContext context,
      AsyncSinkWriterConfiguration configuration,
      Collection<BufferedRequestState<AppendRecord>> bufferedRequestStates,
      Properties s2ConfigProperties,
      String basin,
      String stream) {
    super(
        elementConverter,
        context,
        configuration,
        bufferedRequestStates,
        s2ConfigProperties,
        basin,
        stream);
  }

  @Override
  protected void submitRequestEntries(
      List<AppendRecord> requestEntries, Consumer<List<AppendRecord>> requestToRetry) {
    super.submitRequestEntries(upsertReduce(requestEntries), requestToRetry);
  }

  // TODO
  private static List<AppendRecord> upsertReduce(List<AppendRecord> requestEntries) {
    return Streams.mapWithIndex(
            requestEntries.stream(),
            (record, idx) -> {
              final var key =
                  // TODO should not be position based
                  Optional.ofNullable(Iterables.get(record.headers, 0, null))
                      .flatMap(
                          header ->
                              header.name().equals(KEY_NAME)
                                  ? Optional.of(header.value())
                                  : Optional.empty())
                      .orElse(ByteString.copyFromUtf8(String.valueOf(idx)));
              return Map.entry(key, Map.entry(idx, record));
            })
        .collect(
            Collectors.toMap(
                Map.Entry::getKey, Entry::getValue, (a, b) -> (a.getKey() < b.getKey()) ? b : a))
        .values()
        .stream()
        .map(Entry::getValue)
        .collect(Collectors.toList());
  }
}
