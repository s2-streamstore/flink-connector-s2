package s2.flink.table;

import javax.annotation.Nullable;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.base.table.sink.AsyncDynamicTableSink;
import org.apache.flink.connector.base.table.sink.AsyncDynamicTableSinkBuilder;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;
import s2.flink.config.S2ClientConfig;
import s2.flink.config.S2SinkConfig;
import s2.flink.sink.S2Sink;
import s2.flink.sink.S2SinkBuilder;
import s2.flink.sink.S2SinkElementConverter;
import s2.types.AppendRecord;

public class S2DynamicTableSink extends AsyncDynamicTableSink<AppendRecord> {

  // TODO maybe also need to track physical data type, if could differ?
  private final DataType consumedDataType;
  private final EncodingFormat<SerializationSchema<RowData>> encodingFormat;
  private final ReadableConfig clientConfiguration;

  protected S2DynamicTableSink(
      @Nullable Integer maxBatchSize,
      @Nullable Integer maxInFlightRequests,
      @Nullable Integer maxBufferedRequests,
      @Nullable Long maxBufferSizeInBytes,
      @Nullable Long maxTimeInBufferMS,
      DataType consumedDataType,
      EncodingFormat<SerializationSchema<RowData>> encodingFormat,
      ReadableConfig clientConfiguration) {
    super(
        maxBatchSize,
        maxInFlightRequests,
        maxBufferedRequests,
        maxBufferSizeInBytes,
        maxTimeInBufferMS);
    this.consumedDataType = consumedDataType;
    this.encodingFormat = encodingFormat;
    this.clientConfiguration = clientConfiguration;
  }

  @Override
  public ChangelogMode getChangelogMode(ChangelogMode changelogMode) {
    return ChangelogMode.insertOnly();
  }

  @Override
  public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
    final SerializationSchema<RowData> serializationSchema =
        encodingFormat.createRuntimeEncoder(context, consumedDataType);

    final S2SinkBuilder<RowData> builder = S2Sink.newBuilder();
    builder.setClientConfiguration(clientConfiguration);
    builder.setElementConverter(new S2SinkElementConverter<>(serializationSchema));

    addAsyncOptionsToSinkBuilder(builder);
    S2Sink<RowData> s2Sink = builder.build();
    return SinkV2Provider.of(s2Sink, 1);
  }

  @Override
  public DynamicTableSink copy() {
    return new S2DynamicTableSink(
        maxBatchSize,
        maxInFlightRequests,
        maxBufferedRequests,
        maxBufferSizeInBytes,
        maxTimeInBufferMS,
        consumedDataType,
        encodingFormat,
        clientConfiguration);
  }

  @Override
  public String asSummaryString() {
    return "S2DynamicTableSink";
  }

  public static class S2DynamicTableSinkBuilder
      extends AsyncDynamicTableSinkBuilder<AppendRecord, S2DynamicTableSinkBuilder> {

    private DataType consumedDataType = null;
    private ReadableConfig clientConfiguration = null;
    private EncodingFormat<SerializationSchema<RowData>> encodingFormat = null;

    public S2DynamicTableSinkBuilder setConsumedDataType(DataType consumedDataType) {
      this.consumedDataType = consumedDataType;
      return this;
    }

    public S2DynamicTableSinkBuilder setClientConfiguration(ReadableConfig clientConfiguration) {
      this.clientConfiguration = clientConfiguration;
      return this;
    }

    public S2DynamicTableSinkBuilder setEncodingFormat(
        EncodingFormat<SerializationSchema<RowData>> encodingFormat) {
      this.encodingFormat = encodingFormat;
      return this;
    }

    @Override
    public S2DynamicTableSink build() {
      return new S2DynamicTableSink(
          getMaxBatchSize(),
          getMaxInFlightRequests(),
          getMaxBufferedRequests(),
          getMaxBufferSizeInBytes(),
          getMaxTimeInBufferMS(),
          Preconditions.checkNotNull(this.consumedDataType, "consumedDataType must be provided"),
          Preconditions.checkNotNull(this.encodingFormat, "encodingFormat must be provided"),
          S2SinkConfig.validateForSink(
              S2ClientConfig.validateForSDKConfig(
                  Preconditions.checkNotNull(
                      this.clientConfiguration, "clientConfiguration must be provided"))));
    }
  }
}
