package s2.flink.sink.upsert;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.base.sink.writer.config.AsyncSinkWriterConfiguration.AsyncSinkWriterConfigurationBuilder;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import s2.flink.sink.S2Sink;
import s2.types.AppendRecord;

public class S2UpsertSink extends S2Sink<RowData> {

  private static final Logger LOG = LoggerFactory.getLogger(S2UpsertSink.class);

  protected S2UpsertSink(
      int maxBatchSize,
      int maxInFlightRequests,
      int maxBufferedRequests,
      long maxBatchSizeInBytes,
      long maxTimeInBufferMS,
      long maxRecordSizeInBytes,
      ElementConverter<RowData, AppendRecord> elementConverter,
      ReadableConfig clientConfiguration) {
    super(
        maxBatchSize,
        maxInFlightRequests,
        maxBufferedRequests,
        maxBatchSizeInBytes,
        maxTimeInBufferMS,
        maxRecordSizeInBytes,
        elementConverter,
        clientConfiguration);
  }

  public static S2UpsertSinkBuilder newBuilder() {
    return new S2UpsertSinkBuilder();
  }

  @Override
  public StatefulSinkWriter<RowData, BufferedRequestState<AppendRecord>> createWriter(
      WriterInitContext initContext) throws IOException {

    LOG.trace("writer created, subtask={}", initContext.getTaskInfo().getIndexOfThisSubtask());

    final AsyncSinkWriterConfigurationBuilder builder = new AsyncSinkWriterConfigurationBuilder();
    builder.setMaxBatchSize(getMaxBatchSize());
    builder.setMaxInFlightRequests(getMaxInFlightRequests());
    builder.setMaxBufferedRequests(getMaxBufferedRequests());
    builder.setMaxBatchSizeInBytes(getMaxBatchSizeInBytes());
    builder.setMaxTimeInBufferMS(getMaxTimeInBufferMS());
    builder.setMaxRecordSizeInBytes(getMaxRecordSizeInBytes());

    return new S2UpsertSinkWriter(
        getElementConverter(),
        initContext,
        builder.build(),
        Collections.emptyList(),
        this.clientConfiguration);
  }

  @Override
  public StatefulSinkWriter<RowData, BufferedRequestState<AppendRecord>> restoreWriter(
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

    return new S2UpsertSinkWriter(
        getElementConverter(), context, builder.build(), recoveredState, this.clientConfiguration);
  }
}
