package s2.flink.sink.upsert;

import static s2.flink.config.S2ClientConfig.validateForSDKConfig;
import static s2.flink.config.S2SinkConfig.validateForSink;

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
        clientConfiguration.isPresent(), "S2 config properties must be provided.");
    return new S2UpsertSink(
        defaultOrChecked(
            getMaxBatchSize(),
            S2AsyncSinkConfig.MAX_BATCH_COUNT,
            v -> v >= 0 && v <= S2AsyncSinkConfig.MAX_BATCH_COUNT),
        Optional.ofNullable(getMaxInFlightRequests())
            .orElse(S2AsyncSinkConfig.DEFAULT_MAX_INFLIGHT_REQUESTS),
        Optional.ofNullable(getMaxBufferedRequests())
            .orElse(S2AsyncSinkConfig.DEFAULT_MAX_BUFFERED_REQUESTS),
        defaultOrChecked(
            getMaxBatchSizeInBytes(),
            (long) S2AsyncSinkConfig.MAX_BATCH_SIZE_BYTES,
            v -> v > 0 && v <= S2AsyncSinkConfig.MAX_BATCH_SIZE_BYTES),
        Optional.ofNullable(getMaxTimeInBufferMS())
            .orElse(S2AsyncSinkConfig.DEFAULT_MAX_TIME_IN_BUFFER_MS),
        defaultOrChecked(
            getMaxRecordSizeInBytes(),
            (long) S2AsyncSinkConfig.MAX_RECORD_SIZE_BYTES,
            v -> v > 0 && v <= S2AsyncSinkConfig.MAX_RECORD_SIZE_BYTES),
        this.elementConverter.get(),
        validateForSink(validateForSDKConfig(clientConfiguration.get())));
  }
}
