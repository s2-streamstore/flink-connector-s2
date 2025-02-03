package s2.flink.source.serialization;

import java.io.IOException;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.DeserializationSchema.InitializationContext;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import s2.types.SequencedRecord;

public class S2ContextWrappingDeserializationSchema<T>
    implements S2ContextDeserializationSchema<S2Context<T>> {

  private final DeserializationSchema<T> deserializationSchema;

  public S2ContextWrappingDeserializationSchema(DeserializationSchema<T> deserializationSchema) {
    this.deserializationSchema = deserializationSchema;
  }

  @Override
  public void open(InitializationContext context) throws Exception {
    deserializationSchema.open(context);
  }

  @Override
  public void deserializeIntoCollector(
      SequencedRecord record, String stream, Long seqNum, Collector<S2Context<T>> output)
      throws IOException {
    output.collect(
        new S2Context<T>(
            stream, seqNum, this.deserializationSchema.deserialize(record.body().toByteArray())));
  }

  @Override
  public TypeInformation<S2Context<T>> getProducedType() {
    return TypeInformation.of(new TypeHint<S2Context<T>>() {});
  }
}
