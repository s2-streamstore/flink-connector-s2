package s2.flink.sink;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.connector.base.sink.AsyncSinkBase;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.base.sink.writer.config.AsyncSinkWriterConfiguration.AsyncSinkWriterConfigurationBuilder;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import s2.types.AppendRecord;

public class S2Sink<InputT> extends AsyncSinkBase<InputT, AppendRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(S2Sink.class);

  protected final Properties s2ConfigProperties;
  protected final String basin;
  protected final String stream;

  protected S2Sink(
      int maxBatchSize,
      int maxInFlightRequests,
      int maxBufferedRequests,
      long maxBatchSizeInBytes,
      long maxTimeInBufferMS,
      long maxRecordSizeInBytes,
      ElementConverter<InputT, AppendRecord> elementConverter,
      Properties s2ConfigProperties,
      String basin,
      String stream) {
    super(
        elementConverter,
        maxBatchSize,
        maxInFlightRequests,
        maxBufferedRequests,
        maxBatchSizeInBytes,
        maxTimeInBufferMS,
        maxRecordSizeInBytes);

    this.s2ConfigProperties = s2ConfigProperties;
    this.basin = basin;
    this.stream = stream;
  }

  public static <InputT> S2SinkBuilder<InputT> newBuilder() {
    return new S2SinkBuilder<>();
  }

  @Override
  public SinkWriter<InputT> createWriter(InitContext context) throws IOException {
    throw new RuntimeException("Not implemented");
  }

  public StatefulSinkWriter<InputT, BufferedRequestState<AppendRecord>> createWriter(
      WriterInitContext initContext) throws IOException {
    LOG.trace("writer created, subtask={}", initContext.getTaskInfo().getIndexOfThisSubtask());

    final AsyncSinkWriterConfigurationBuilder builder = new AsyncSinkWriterConfigurationBuilder();
    builder.setMaxBatchSize(getMaxBatchSize());
    builder.setMaxInFlightRequests(getMaxInFlightRequests());
    builder.setMaxBufferedRequests(getMaxBufferedRequests());
    builder.setMaxBatchSizeInBytes(getMaxBatchSizeInBytes());
    builder.setMaxTimeInBufferMS(getMaxTimeInBufferMS());
    builder.setMaxRecordSizeInBytes(getMaxRecordSizeInBytes());

    return new S2SinkWriter<>(
        getElementConverter(),
        initContext,
        builder.build(),
        Collections.emptyList(),
        this.s2ConfigProperties,
        this.basin,
        this.stream);
  }

  @Override
  public StatefulSinkWriter<InputT, BufferedRequestState<AppendRecord>> restoreWriter(
      WriterInitContext context, Collection<BufferedRequestState<AppendRecord>> recoveredState)
      throws IOException {
    LOG.trace("writer restored, subtask={}", context.getTaskInfo().getIndexOfThisSubtask());

    final AsyncSinkWriterConfigurationBuilder builder = new AsyncSinkWriterConfigurationBuilder();
    builder.setMaxBatchSize(getMaxBatchSize());
    builder.setMaxInFlightRequests(getMaxInFlightRequests());
    builder.setMaxBufferedRequests(getMaxBufferedRequests());
    builder.setMaxBatchSizeInBytes(getMaxBatchSizeInBytes());
    builder.setMaxTimeInBufferMS(getMaxTimeInBufferMS());
    builder.setMaxRecordSizeInBytes(getMaxRecordSizeInBytes());

    return new S2SinkWriter<InputT>(
        getElementConverter(),
        context,
        builder.build(),
        recoveredState,
        this.s2ConfigProperties,
        this.basin,
        this.stream);
  }

  @Override
  public SimpleVersionedSerializer<BufferedRequestState<AppendRecord>> getWriterStateSerializer() {
    return new S2StateSerializer();
  }
}
