package s2.flink.source.reader;

import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.util.Collector;
import s2.flink.source.serialization.S2ContextDeserializationSchema;
import s2.flink.source.split.S2SourceSplitState;
import s2.types.SequencedRecord;

public class S2RecordEmitter<T> implements RecordEmitter<SequencedRecord, T, S2SourceSplitState> {

  private final S2ContextDeserializationSchema<T> deserializationSchema;

  public S2RecordEmitter(S2ContextDeserializationSchema<T> deserializationSchema) {
    this.deserializationSchema = deserializationSchema;
  }

  @Override
  public void emitRecord(
      SequencedRecord element, SourceOutput<T> output, S2SourceSplitState splitState)
      throws Exception {
    deserializationSchema.deserializeIntoCollector(
        element,
        splitState.originalSplit.splitId(),
        element.seqNum,
        new SourceOutputWrapper<>(output));
    splitState.register(element);
  }

  private static class SourceOutputWrapper<T> implements Collector<T> {

    private final SourceOutput<T> sourceOutput;

    public SourceOutputWrapper(SourceOutput<T> sourceOutput) {
      this.sourceOutput = sourceOutput;
    }

    @Override
    public void collect(T record) {
      sourceOutput.collect(record);
    }

    @Override
    public void close() {}
  }
}
