package s2.flink.sink;

import static s2.flink.config.S2ClientConfig.validateForSDKConfig;
import static s2.flink.config.S2SinkConfig.validateForSink;

import java.util.Optional;
import java.util.function.Predicate;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.base.sink.AsyncSinkBaseBuilder;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.util.Preconditions;
import s2.types.AppendRecord;

public class S2SinkBuilder<InputT>
    extends AsyncSinkBaseBuilder<InputT, AppendRecord, S2SinkBuilder<InputT>> {

  protected Optional<ElementConverter<InputT, AppendRecord>> elementConverter = Optional.empty();
  protected Optional<ReadableConfig> clientConfiguration = Optional.empty();

  public S2SinkBuilder<InputT> setClientConfiguration(ReadableConfig properties) {
    this.clientConfiguration = Optional.of(properties);
    return this;
  }

  public S2SinkBuilder<InputT> setElementConverter(
      ElementConverter<InputT, AppendRecord> elementConverter) {
    this.elementConverter = Optional.of(elementConverter);
    return this;
  }

  @Override
  public S2Sink<InputT> build() {
    Preconditions.checkArgument(
        elementConverter.isPresent(), "Element converter must be provided.");
    Preconditions.checkArgument(
        clientConfiguration.isPresent(), "S2 config properties must be provided.");
    return new S2Sink<>(
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
        validateForSDKConfig(validateForSink(clientConfiguration.get())));
  }

  protected static <T> T defaultOrChecked(T candidate, T defaultValue, Predicate<T> validator) {
    final T val = Optional.ofNullable(candidate).orElse(defaultValue);
    if (validator.test(val)) {
      return val;
    } else {
      throw new IllegalArgumentException("Value does not satisfy validator: " + validator);
    }
  }
}
