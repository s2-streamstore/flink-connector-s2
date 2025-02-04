package s2.flink.sink;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter.Context;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.SimpleUserCodeClassLoader;
import org.apache.flink.util.UserCodeClassLoader;
import s2.types.AppendRecord;

public class S2SinkElementConverter<InputT> implements ElementConverter<InputT, AppendRecord> {

  final SerializationSchema<InputT> serializationSchema;

  public S2SinkElementConverter(SerializationSchema<InputT> serializationSchema) {
    this.serializationSchema = serializationSchema;
  }

  @Override
  public AppendRecord apply(InputT element, Context context) {
    return AppendRecord.newBuilder().withBody(serializationSchema.serialize(element)).build();
  }

  @Override
  public void open(Sink.InitContext context) {
    try {
      serializationSchema.open(
          new SerializationSchema.InitializationContext() {
            @Override
            public MetricGroup getMetricGroup() {
              return new UnregisteredMetricsGroup();
            }

            @Override
            public UserCodeClassLoader getUserCodeClassLoader() {
              return SimpleUserCodeClassLoader.create(
                  S2SinkElementConverter.class.getClassLoader());
            }
          });
    } catch (Exception e) {
      throw new FlinkRuntimeException("Failed to initialize serialization schema.", e);
    }
  }
}
