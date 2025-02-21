package s2.flink.sink.upsert;

import static s2.flink.record.Upsert.extractKeyAndAction;

import com.google.common.collect.Streams;
import com.google.protobuf.ByteString;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.configuration.ReadableConfig;
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
      ReadableConfig clientConfiguration) {
    super(elementConverter, context, configuration, bufferedRequestStates, clientConfiguration);
  }

  @Override
  protected void submitRequestEntries(
      List<AppendRecord> requestEntries, Consumer<List<AppendRecord>> requestToRetry) {
    super.submitRequestEntries(upsertReduce(requestEntries), requestToRetry);
  }

  private static List<AppendRecord> upsertReduce(List<AppendRecord> requestEntries) {
    return Streams.mapWithIndex(
            requestEntries.stream(),
            (record, idx) -> {
              final var key =
                  extractKeyAndAction(record.headers)
                      .map(kv -> kv.f0)
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
