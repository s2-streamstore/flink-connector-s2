package s2.flink.table;

import javax.annotation.Nullable;
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
import s2.flink.source.S2Source;

public class S2DynamicTableSource implements ScanTableSource {

  private final DecodingFormat<DeserializationSchema<RowData>> decodingFormat;
  private final DataType physicalDataType;
  private final Configuration sourceConfig;
  private final DataType producedDataType;

  public S2DynamicTableSource(
      @Nullable DataType physicalDataType,
      Configuration sourceConfig,
      DecodingFormat<DeserializationSchema<RowData>> decodingFormat,
      DataType producedDataType) {
    this.physicalDataType = physicalDataType;
    this.sourceConfig = sourceConfig;
    this.decodingFormat = decodingFormat;
    this.producedDataType = producedDataType;
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

        final S2Source<RowData> source = new S2Source<>(sourceConfig, deserializationSchema);

        return execEnv.fromSource(source, WatermarkStrategy.noWatermarks(), "S2Source");
      }

      @Override
      public boolean isBounded() {
        return false;
      }
    };
  }

  @Override
  public DynamicTableSource copy() {
    return null;
  }

  @Override
  public String asSummaryString() {
    return "S2DynamicTableSource";
  }
}
