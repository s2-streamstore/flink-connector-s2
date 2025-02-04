package s2.flink.table;

import java.util.Properties;
import javax.annotation.Nullable;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.base.table.sink.AsyncDynamicTableSink;
import org.apache.flink.connector.base.table.sink.AsyncDynamicTableSinkBuilder;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;
import s2.flink.sink.S2Sink;
import s2.flink.sink.S2SinkBuilder;
import s2.flink.sink.S2SinkElementConverter;
import s2.types.AppendRecord;

public class S2DynamicTableSink extends AsyncDynamicTableSink<AppendRecord> {

  // TODO maybe also need to track physical data type, if could differ?
  private final DataType consumedDataType;
  private final EncodingFormat<SerializationSchema<RowData>> encodingFormat;
  private final Properties s2ClientProperties;
  private final String basin;
  private final String stream;

  protected S2DynamicTableSink(
      @Nullable Integer maxBatchSize,
      @Nullable Integer maxInFlightRequests,
      @Nullable Integer maxBufferedRequests,
      @Nullable Long maxBufferSizeInBytes,
      @Nullable Long maxTimeInBufferMS,
      DataType consumedDataType,
      EncodingFormat<SerializationSchema<RowData>> encodingFormat,
      Properties s2ClientProperties,
      String basin,
      String stream) {
    super(
        maxBatchSize,
        maxInFlightRequests,
        maxBufferedRequests,
        maxBufferSizeInBytes,
        maxTimeInBufferMS);
    this.consumedDataType = consumedDataType;
    this.encodingFormat = encodingFormat;
    this.s2ClientProperties = s2ClientProperties;
    this.basin = basin;
    this.stream = stream;
  }

  @Override
  public ChangelogMode getChangelogMode(ChangelogMode changelogMode) {
    return ChangelogMode.insertOnly();
  }

  @Override
  public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
    SerializationSchema<RowData> serializationSchema =
        encodingFormat.createRuntimeEncoder(context, consumedDataType);

    final S2SinkBuilder<RowData> builder = S2Sink.newBuilder();
    builder.setS2ConfigProperties(s2ClientProperties);
    builder.setBasin(basin);
    builder.setStream(stream);
    builder.setElementConverter(new S2SinkElementConverter<>(serializationSchema));

    addAsyncOptionsToSinkBuilder(builder);
    S2Sink<RowData> s2Sink = builder.build();
    return SinkV2Provider.of(s2Sink);
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
        s2ClientProperties,
        basin,
        stream);
  }

  @Override
  public String asSummaryString() {
    return "S2DynamicTableSink";
  }

  public static class S2DynamicTableSinkBuilder
      extends AsyncDynamicTableSinkBuilder<AppendRecord, S2DynamicTableSinkBuilder> {

    private DataType consumedDataType = null;
    private Properties s2ClientProperties = null;
    private String basin = null;
    private String stream = null;
    private EncodingFormat<SerializationSchema<RowData>> encodingFormat = null;

    public S2DynamicTableSinkBuilder setConsumedDataType(DataType consumedDataType) {
      this.consumedDataType = consumedDataType;
      return this;
    }

    public S2DynamicTableSinkBuilder setS2ClientProperties(Properties s2ClientProperties) {
      this.s2ClientProperties = s2ClientProperties;
      return this;
    }

    public S2DynamicTableSinkBuilder setBasin(String basin) {
      this.basin = basin;
      return this;
    }

    public S2DynamicTableSinkBuilder setStream(String stream) {
      this.stream = stream;
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
          Preconditions.checkNotNull(
              this.s2ClientProperties, "s2ClientProperties must be provided"),
          Preconditions.checkNotNull(basin, "s2 basin must be provided"),
          Preconditions.checkNotNull(stream, "s2 stream must be provided"));
    }
  }
}
