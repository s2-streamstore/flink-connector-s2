package s2.flink.table.upsert;

import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.base.table.sink.AsyncDynamicTableSink;
import org.apache.flink.connector.base.table.sink.AsyncDynamicTableSinkBuilder;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.RowData.FieldGetter;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.Preconditions;
import s2.flink.config.S2ClientConfig;
import s2.flink.config.S2SinkConfig;
import s2.flink.sink.upsert.S2UpsertSink;
import s2.flink.sink.upsert.S2UpsertSinkBuilder;
import s2.flink.sink.upsert.S2UpsertSinkRowDataConverter;
import s2.types.AppendRecord;

public class S2UpsertDynamicTableSink extends AsyncDynamicTableSink<AppendRecord> {

  // TODO maybe also need to track physical data type, if could differ?
  private final DataType consumedDataType;
  private final EncodingFormat<SerializationSchema<RowData>> encodingFormat;
  private final ReadableConfig clientConfiguration;
  private final Tuple2<int[], int[]> kvRowIndices;

  protected S2UpsertDynamicTableSink(
      @Nullable Integer maxBatchSize,
      @Nullable Integer maxInFlightRequests,
      @Nullable Integer maxBufferedRequests,
      @Nullable Long maxBufferSizeInBytes,
      @Nullable Long maxTimeInBufferMS,
      DataType consumedDataType,
      EncodingFormat<SerializationSchema<RowData>> encodingFormat,
      ReadableConfig clientConfiguration,
      Tuple2<int[], int[]> kvRowIndices) {
    super(
        maxBatchSize,
        maxInFlightRequests,
        maxBufferedRequests,
        maxBufferSizeInBytes,
        maxTimeInBufferMS);
    this.consumedDataType = consumedDataType;
    this.encodingFormat = encodingFormat;
    this.clientConfiguration = clientConfiguration;
    this.kvRowIndices = kvRowIndices;
  }

  @Override
  public ChangelogMode getChangelogMode(ChangelogMode changelogMode) {
    return ChangelogMode.upsert();
  }

  @Override
  public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
    // TODO Use a wrapped upsert serializer if enabled?
    SerializationSchema<RowData> serializationSchema =
        encodingFormat.createRuntimeEncoder(context, consumedDataType);

    SerializationSchema<RowData> keySerializer =
        createSerialization(context, encodingFormat, this.kvRowIndices.f0);
    SerializationSchema<RowData> valueSerializer =
        createSerialization(context, encodingFormat, this.kvRowIndices.f1);

    final S2UpsertSinkBuilder builder = S2UpsertSink.newBuilder();
    builder.setClientConfiguration(clientConfiguration);

    final List<LogicalType> physicalDataTypeChildren =
        consumedDataType.getLogicalType().getChildren();
    final FieldGetter[] keyFieldGetters =
        getFieldGetters(physicalDataTypeChildren, kvRowIndices.f0);
    final FieldGetter[] valueFieldGetters =
        getFieldGetters(physicalDataTypeChildren, kvRowIndices.f1);
    builder.setElementConverter(
        new S2UpsertSinkRowDataConverter(
            keySerializer, valueSerializer, keyFieldGetters, valueFieldGetters));

    addAsyncOptionsToSinkBuilder(builder);
    S2UpsertSink s2Sink = builder.build();
    return SinkV2Provider.of(s2Sink, 1);
  }

  private @Nullable SerializationSchema<RowData> createSerialization(
      DynamicTableSink.Context context,
      @Nullable EncodingFormat<SerializationSchema<RowData>> format,
      int[] projection) {
    if (format == null) {
      return null;
    }
    DataType physicalFormatDataType = Projection.of(projection).project(this.consumedDataType);
    return format.createRuntimeEncoder(context, physicalFormatDataType);
  }

  private RowData.FieldGetter[] getFieldGetters(
      List<LogicalType> physicalDataTypeChildren, int[] fieldIndices) {
    return Arrays.stream(fieldIndices)
        .mapToObj(
            targetField ->
                RowData.createFieldGetter(physicalDataTypeChildren.get(targetField), targetField))
        .toArray(RowData.FieldGetter[]::new);
  }

  @Override
  public DynamicTableSink copy() {
    return new S2UpsertDynamicTableSink(
        maxBatchSize,
        maxInFlightRequests,
        maxBufferedRequests,
        maxBufferSizeInBytes,
        maxTimeInBufferMS,
        consumedDataType,
        encodingFormat,
        clientConfiguration,
        kvRowIndices);
  }

  @Override
  public String asSummaryString() {
    return "S2UpsertDynamicTableSink";
  }

  public static class S2UpsertDynamicTableSinkBuilder
      extends AsyncDynamicTableSinkBuilder<AppendRecord, S2UpsertDynamicTableSinkBuilder> {

    private DataType consumedDataType = null;
    private ReadableConfig clientConfiguration = null;
    private EncodingFormat<SerializationSchema<RowData>> encodingFormat = null;
    private String basin = null;
    private String stream = null;
    private int[] keyIndices = new int[0];
    private int[] valueIndices = new int[0];

    public S2UpsertDynamicTableSinkBuilder setConsumedDataType(DataType consumedDataType) {
      this.consumedDataType = consumedDataType;
      return this;
    }

    public S2UpsertDynamicTableSinkBuilder setClientConfiguration(
        ReadableConfig clientConfiguration) {
      this.clientConfiguration = clientConfiguration;
      return this;
    }

    public S2UpsertDynamicTableSinkBuilder setBasin(String basin) {
      this.basin = basin;
      return this;
    }

    public S2UpsertDynamicTableSinkBuilder setStream(String stream) {
      this.stream = stream;
      return this;
    }

    public S2UpsertDynamicTableSinkBuilder setKeyValueRowIndices(
        int[] keyIndices, int[] valueIndices) {
      this.keyIndices = keyIndices;
      this.valueIndices = valueIndices;
      return this;
    }

    public S2UpsertDynamicTableSinkBuilder setEncodingFormat(
        EncodingFormat<SerializationSchema<RowData>> encodingFormat) {
      this.encodingFormat = encodingFormat;
      return this;
    }

    @Override
    public S2UpsertDynamicTableSink build() {
      Preconditions.checkArgument(keyIndices.length > 0);
      Preconditions.checkArgument(valueIndices.length > 0);
      return new S2UpsertDynamicTableSink(
          getMaxBatchSize(),
          getMaxInFlightRequests(),
          getMaxBufferedRequests(),
          getMaxBufferSizeInBytes(),
          getMaxTimeInBufferMS(),
          this.consumedDataType,
          Preconditions.checkNotNull(this.encodingFormat, "encodingFormat must be provided"),
          S2ClientConfig.validateForSDKConfig(
              S2SinkConfig.validateForSink(
                  Preconditions.checkNotNull(
                      this.clientConfiguration, "clientConfiguration must be provided"))),
          Tuple2.of(keyIndices, valueIndices));
    }
  }
}
