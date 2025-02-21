package s2.flink.table.upsert;

import static s2.flink.record.Upsert.extractKeyAndAction;

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.DeserializationSchema.InitializationContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import s2.flink.source.serialization.S2ContextDeserializationSchema;
import s2.types.SequencedRecord;

public class UpsertRowDataDeserializer implements S2ContextDeserializationSchema<RowData> {

  private final DeserializationSchema<RowData> keyDeserializer;
  private final DeserializationSchema<RowData> valueDeserializer;
  private final int[] keyProjection;
  private final int[] valueProjection;
  private final TypeInformation<RowData> typeInfo;
  private final int physicalArity;

  private final RowData.FieldGetter[] keyFieldGetters;
  private final RowData.FieldGetter[] valueFieldGetters;

  public UpsertRowDataDeserializer(
      DataType physicalDataType,
      DeserializationSchema<RowData> keyDeserializer,
      DeserializationSchema<RowData> valueDeserializer,
      int[] keyProjection,
      int[] valueProjection,
      TypeInformation<RowData> typeInfo) {
    this.keyDeserializer = keyDeserializer;
    this.valueDeserializer = valueDeserializer;
    this.keyProjection = keyProjection;
    this.valueProjection = valueProjection;
    this.typeInfo = typeInfo;
    this.physicalArity = DataType.getFieldDataTypes(physicalDataType).size();
    this.keyFieldGetters =
        getFieldGetters(
            Projection.of(keyProjection).project(physicalDataType).getLogicalType().getChildren(),
            IntStream.range(0, keyProjection.length).toArray());
    this.valueFieldGetters =
        getFieldGetters(
            Projection.of(valueProjection).project(physicalDataType).getLogicalType().getChildren(),
            IntStream.range(0, valueProjection.length).toArray());
  }

  private RowData.FieldGetter[] getFieldGetters(
      List<LogicalType> physicalDataTypeChildren, int[] fieldIndices) {
    return Arrays.stream(fieldIndices)
        .mapToObj(
            targetField ->
                RowData.createFieldGetter(physicalDataTypeChildren.get(targetField), targetField))
        .toArray(RowData.FieldGetter[]::new);
  }

  @Override
  public TypeInformation<RowData> getProducedType() {
    return typeInfo;
  }

  @Override
  public void open(InitializationContext context) throws Exception {
    keyDeserializer.open(context);
    valueDeserializer.open(context);
  }

  @Override
  public void deserializeIntoCollector(
      SequencedRecord record, String stream, Long seqNum, Collector<RowData> output)
      throws IOException {
    // (Most similar to Kafka's DynamicKafkaDeserializationSchema.emitRow).

    final Tuple2<ByteString, RowKind> keyAndAction =
        extractKeyAndAction(record.headers)
            .orElseThrow(() -> new RuntimeException("key or rowkind not found in record headers"));

    final RowData key = keyDeserializer.deserialize(keyAndAction.f0.toByteArray());
    final RowData value = valueDeserializer.deserialize(record.body.toByteArray());

    final GenericRowData generated = new GenericRowData(keyAndAction.f1, physicalArity);

    for (int keyPos = 0; keyPos < keyProjection.length; keyPos++) {
      generated.setField(keyPos, keyFieldGetters[keyPos].getFieldOrNull(key));
    }

    if (value != null) {
      for (int valuePos = 0; valuePos < valueProjection.length; valuePos++) {
        generated.setField(
            keyProjection.length + valuePos, valueFieldGetters[valuePos].getFieldOrNull(value));
      }
    }

    output.collect(generated);
  }
}
