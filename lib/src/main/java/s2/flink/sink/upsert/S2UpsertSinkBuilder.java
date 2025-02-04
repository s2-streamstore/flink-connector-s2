package s2.flink.sink.upsert;

import java.util.Optional;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;
import s2.flink.sink.S2AsyncSinkConfig;
import s2.flink.sink.S2SinkBuilder;

public class S2UpsertSinkBuilder extends S2SinkBuilder<RowData> {

  @Override
  public S2UpsertSink build() {
    Preconditions.checkArgument(
        elementConverter.isPresent(), "Element converter must be provided.");
    Preconditions.checkArgument(
        s2ConfigProperties.isPresent(), "S2 config properties must be provided.");
    Preconditions.checkArgument(stream.isPresent(), "Stream must be provided.");
    Preconditions.checkArgument(basin.isPresent(), "Basin must be provided.");
    return new S2UpsertSink(
        Optional.ofNullable(getMaxBatchSize()).orElse(S2AsyncSinkConfig.MAX_BATCH_COUNT),
        Optional.ofNullable(getMaxInFlightRequests())
            .orElse(S2AsyncSinkConfig.DEFAULT_MAX_INFLIGHT_REQUESTS),
        Optional.ofNullable(getMaxBufferedRequests())
            .orElse(S2AsyncSinkConfig.DEFAULT_MAX_BUFFERED_REQUESTS),
        Optional.ofNullable(getMaxBatchSizeInBytes())
            .orElse((long) S2AsyncSinkConfig.MAX_BATCH_SIZE_BYTES),
        Optional.ofNullable(getMaxTimeInBufferMS())
            .orElse(S2AsyncSinkConfig.DEFAULT_MAX_TIME_IN_BUFFER_MS),
        Optional.ofNullable(getMaxRecordSizeInBytes())
            .orElse((long) S2AsyncSinkConfig.MAX_RECORD_SIZE_BYTES),
        this.elementConverter.get(),
        s2ConfigProperties.get(),
        this.basin.get(),
        this.stream.get());
  }
}
