package s2.flink.sink;

import java.util.Optional;
import java.util.Properties;
import org.apache.flink.connector.base.sink.AsyncSinkBaseBuilder;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.util.Preconditions;
import s2.types.AppendRecord;

public class S2SinkBuilder<InputT>
    extends AsyncSinkBaseBuilder<InputT, AppendRecord, S2SinkBuilder<InputT>> {

  protected Optional<ElementConverter<InputT, AppendRecord>> elementConverter = Optional.empty();
  protected Optional<Properties> s2ConfigProperties = Optional.empty();
  protected Optional<String> basin = Optional.empty();
  protected Optional<String> stream = Optional.empty();

  public S2SinkBuilder<InputT> setS2ConfigProperties(Properties properties) {
    this.s2ConfigProperties = Optional.of(properties);
    return this;
  }

  public S2SinkBuilder<InputT> setElementConverter(
      ElementConverter<InputT, AppendRecord> elementConverter) {
    this.elementConverter = Optional.of(elementConverter);
    return this;
  }

  public S2SinkBuilder<InputT> setBasin(String basin) {
    this.basin = Optional.of(basin);
    return this;
  }

  public S2SinkBuilder<InputT> setStream(String stream) {
    this.stream = Optional.of(stream);
    return this;
  }

  @Override
  public S2Sink<InputT> build() {
    Preconditions.checkArgument(
        elementConverter.isPresent(), "Element converter must be provided.");
    Preconditions.checkArgument(
        s2ConfigProperties.isPresent(), "S2 config properties must be provided.");
    Preconditions.checkArgument(stream.isPresent(), "Stream must be provided.");
    Preconditions.checkArgument(basin.isPresent(), "Basin must be provided.");
    return new S2Sink<>(
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
