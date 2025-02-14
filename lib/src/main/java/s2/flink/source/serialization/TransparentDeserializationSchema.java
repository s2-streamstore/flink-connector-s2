package s2.flink.source.serialization;

import java.io.IOException;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.DeserializationSchema.InitializationContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import s2.types.SequencedRecord;

public class TransparentDeserializationSchema<T> implements S2ContextDeserializationSchema<T> {

  private final DeserializationSchema<T> coreSchema;

  public TransparentDeserializationSchema(DeserializationSchema<T> coreSchema) {
    this.coreSchema = coreSchema;
  }

  @Override
  public void open(InitializationContext context) throws Exception {
    this.coreSchema.open(context);
  }

  @Override
  public void deserializeIntoCollector(
      SequencedRecord record, String stream, Long seqNum, Collector<T> output) throws IOException {
    // Ignore stream, seqNum...
    output.collect(this.coreSchema.deserialize(record.body.toByteArray()));
  }

  @Override
  public TypeInformation<T> getProducedType() {
    return this.coreSchema.getProducedType();
  }
}
