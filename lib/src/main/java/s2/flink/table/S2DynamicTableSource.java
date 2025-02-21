package s2.flink.table;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;
import s2.flink.config.S2ClientConfig;
import s2.flink.config.S2SourceConfig;
import s2.flink.source.S2Source;

public class S2DynamicTableSource implements ScanTableSource {

  private final DecodingFormat<DeserializationSchema<RowData>> decodingFormat;
  private final DataType physicalDataType;
  private final Configuration sourceConfig;
  private final DataType producedDataType;

  private S2DynamicTableSource(
      DataType physicalDataType,
      Configuration sourceConfig,
      DecodingFormat<DeserializationSchema<RowData>> decodingFormat,
      DataType producedDataType) {
    this.physicalDataType = physicalDataType;
    this.sourceConfig = sourceConfig;
    this.decodingFormat = decodingFormat;
    this.producedDataType = producedDataType;
  }

  public static S2DynamicTableSourceBuilder newBuilder() {
    return new S2DynamicTableSourceBuilder();
  }

  @Override
  public ChangelogMode getChangelogMode() {
    return ChangelogMode.insertOnly();
  }

  @Override
  public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
    DeserializationSchema<RowData> deserializationSchema =
        decodingFormat.createRuntimeDecoder(runtimeProviderContext, physicalDataType);

    return new DataStreamScanProvider() {
      @Override
      public DataStream<RowData> produceDataStream(
          ProviderContext providerContext, StreamExecutionEnvironment execEnv) {

        return execEnv.fromSource(
            new S2Source<RowData>(sourceConfig, deserializationSchema),
            WatermarkStrategy.noWatermarks(),
            "S2Source");
      }

      @Override
      public boolean isBounded() {
        return false;
      }
    };
  }

  @Override
  public DynamicTableSource copy() {
    return new S2DynamicTableSource(
        producedDataType, sourceConfig, decodingFormat, producedDataType);
  }

  @Override
  public String asSummaryString() {
    return "S2DynamicTableSource";
  }

  public static class S2DynamicTableSourceBuilder {

    private DecodingFormat<DeserializationSchema<RowData>> decodingFormat;
    private DataType physicalDataType;
    private Configuration sourceConfig;
    private DataType producedDataType;

    public S2DynamicTableSourceBuilder setDecodingFormat(
        DecodingFormat<DeserializationSchema<RowData>> decodingFormat) {
      this.decodingFormat = decodingFormat;
      return this;
    }

    public S2DynamicTableSourceBuilder setPhysicalDataType(DataType physicalDataType) {
      this.physicalDataType = physicalDataType;
      return this;
    }

    public S2DynamicTableSourceBuilder setSourceConfig(Configuration sourceConfig) {
      this.sourceConfig = sourceConfig;
      return this;
    }

    public S2DynamicTableSourceBuilder setProducedDataType(DataType producedDataType) {
      this.producedDataType = producedDataType;
      return this;
    }

    public S2DynamicTableSource build() {
      return new S2DynamicTableSource(
          Preconditions.checkNotNull(physicalDataType, "physicalDataType must be provided"),
          S2SourceConfig.validateForSource(
              S2ClientConfig.validateForSDKConfig(
                  Preconditions.checkNotNull(sourceConfig, "sourceConfig must be provided"))),
          Preconditions.checkNotNull(decodingFormat, "decodingFormat must be provided"),
          Preconditions.checkNotNull(producedDataType, "producedDataType must be provided"));
    }
  }
}
