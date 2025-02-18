package s2.flink.sink.upsert;

import static s2.flink.record.Upsert.keyDelete;
import static s2.flink.record.Upsert.keyUpdate;

import com.google.protobuf.ByteString;
import java.util.List;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema.InitializationContext;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter.Context;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.RowData.FieldGetter;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.SimpleUserCodeClassLoader;
import org.apache.flink.util.UserCodeClassLoader;
import s2.flink.sink.S2SinkElementConverter;
import s2.types.AppendRecord;
import s2.types.Header;

public class S2UpsertSinkRowDataConverter implements ElementConverter<RowData, AppendRecord> {

  private final FieldGetter[] keyFieldGetters;
  private final FieldGetter[] valueFieldGetters;
  private final SerializationSchema<RowData> keySerializer;
  private final SerializationSchema<RowData> valueSerializer;

  public S2UpsertSinkRowDataConverter(
      SerializationSchema<RowData> keySerializer,
      SerializationSchema<RowData> valueSerializer,
      FieldGetter[] keyFieldGetters,
      FieldGetter[] valueFieldGetters) {
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
    this.keyFieldGetters = keyFieldGetters;
    this.valueFieldGetters = valueFieldGetters;
  }

  @Override
  public AppendRecord apply(RowData combinedElement, Context context) {
    final RowKind inputKind = combinedElement.getRowKind();

    final ByteString serializedKey =
        ByteString.copyFrom(
            keySerializer.serialize(
                createProjectedRow(combinedElement, RowKind.INSERT, keyFieldGetters)));

    final ByteString serializedBody;
    final List<Header> headers;
    if (inputKind == RowKind.DELETE) {
      serializedBody = ByteString.empty();
      headers = keyDelete(serializedKey);
    } else if (inputKind == RowKind.INSERT || inputKind == RowKind.UPDATE_AFTER) {
      serializedBody =
          ByteString.copyFrom(
              valueSerializer.serialize(
                  createProjectedRow(combinedElement, RowKind.INSERT, valueFieldGetters)));
      headers = keyUpdate(serializedKey);
    } else {
      throw new IllegalArgumentException("Unsupported row kind: " + inputKind);
    }

    return AppendRecord.newBuilder().withHeaders(headers).withBody(serializedBody).build();
  }

  static GenericRowData createProjectedRow(
      RowData consumedRow, RowKind kind, RowData.FieldGetter[] fieldGetters) {
    final int arity = fieldGetters.length;
    final GenericRowData genericRowData = new GenericRowData(kind, arity);
    for (int fieldPos = 0; fieldPos < arity; fieldPos++) {
      genericRowData.setField(fieldPos, fieldGetters[fieldPos].getFieldOrNull(consumedRow));
    }
    return genericRowData;
  }

  @Override
  public void open(Sink.InitContext context) {
    final InitializationContext initContext =
        new SerializationSchema.InitializationContext() {
          @Override
          public MetricGroup getMetricGroup() {
            return new UnregisteredMetricsGroup();
          }

          @Override
          public UserCodeClassLoader getUserCodeClassLoader() {
            return SimpleUserCodeClassLoader.create(S2SinkElementConverter.class.getClassLoader());
          }
        };
    try {
      keySerializer.open(initContext);
      valueSerializer.open(initContext);
    } catch (Exception e) {
      throw new FlinkRuntimeException("Failed to initialize serialization schema.", e);
    }
  }
}
