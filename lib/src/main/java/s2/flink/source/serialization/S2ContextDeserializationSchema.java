package s2.flink.source.serialization;

import java.io.IOException;
import java.io.Serializable;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.util.Collector;
import s2.types.SequencedRecord;

public interface S2ContextDeserializationSchema<T> extends Serializable, ResultTypeQueryable<T> {

  static <T> S2ContextDeserializationSchema<T> of(DeserializationSchema<T> deserializationSchema) {
    return new TransparentDeserializationSchema<>(deserializationSchema);
  }

  void open(DeserializationSchema.InitializationContext context) throws Exception;

  void deserializeIntoCollector(
      SequencedRecord record, String stream, Long seqNum, Collector<T> output) throws IOException;
}
