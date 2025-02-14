package s2.flink.table.upsert;

import com.google.protobuf.ByteString;
import java.io.IOException;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.DeserializationSchema.InitializationContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import s2.flink.source.serialization.S2ContextDeserializationSchema;
import s2.types.SequencedRecord;

public class UpsertRowDataDeserializer implements S2ContextDeserializationSchema<RowData> {
  private static final ByteString FLAG_UPDATE = ByteString.copyFromUtf8("u");
  private static final ByteString FLAG_DELETE = ByteString.copyFromUtf8("d");
  private final DataType physicalDataType;
  private final DeserializationSchema<RowData> keyDeserializer;
  private final DeserializationSchema<RowData> valueDeserializer;
  private final int[] keyProjection;
  private final int[] valueProjection;
  private final TypeInformation<RowData> typeInfo;
  private final int physicalArity;

  public UpsertRowDataDeserializer(
      DataType physicalDataType,
      DeserializationSchema<RowData> keyDeserializer,
      DeserializationSchema<RowData> valueDeserializer,
      int[] keyProjection,
      int[] valueProjection,
      TypeInformation<RowData> typeInfo) {
    this.physicalDataType = physicalDataType;
    this.keyDeserializer = keyDeserializer;
    this.valueDeserializer = valueDeserializer;
    this.keyProjection = keyProjection;
    this.valueProjection = valueProjection;
    this.typeInfo = typeInfo;
    this.physicalArity = DataType.getFieldDataTypes(physicalDataType).size();
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
    // Most similar to Kafka's DynamicKafkaDeserializationSchema.emitRow

    // TODO no casting
    final GenericRowData key =
        (GenericRowData) keyDeserializer.deserialize(record.headers.get(0).value.toByteArray());
    final GenericRowData value =
        (GenericRowData) valueDeserializer.deserialize(record.body.toByteArray());

    // TODO search for proper header
    final RowKind recovered;
    final var action = record.headers.get(1).value;
    if (action.equals(FLAG_UPDATE)) {
      recovered = RowKind.UPDATE_AFTER;
    } else if (action.equals(FLAG_DELETE)) {
      recovered = RowKind.DELETE;
    } else {
      throw new RuntimeException("Unknown action: " + action);
    }

    final GenericRowData generated = new GenericRowData(recovered, physicalArity);

    for (int keyPos = 0; keyPos < keyProjection.length; keyPos++) {
      generated.setField(keyProjection[keyPos], key.getField(keyPos));
    }

    if (value != null) {
      for (int valuePos = 0; valuePos < valueProjection.length; valuePos++) {
        generated.setField(valueProjection[valuePos], value.getField(valuePos));
      }
    }

    output.collect(generated);
  }
}
