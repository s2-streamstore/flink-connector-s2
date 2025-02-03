package s2.flink.source;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.UserCodeClassLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import s2.flink.source.enumerator.S2EnumeratorState;
import s2.flink.source.enumerator.S2EnumeratorStateSerializer;
import s2.flink.source.enumerator.S2SplitEnumerator;
import s2.flink.source.reader.S2RecordEmitter;
import s2.flink.source.reader.S2SourceReader;
import s2.flink.source.reader.S2SplitReader;
import s2.flink.source.serialization.S2ContextDeserializationSchema;
import s2.flink.source.split.S2SourceSplit;
import s2.flink.source.split.S2SplitSerializer;

public class S2Source<T> implements Source<T, S2SourceSplit, S2EnumeratorState> {

  private static final Logger LOG = LoggerFactory.getLogger(S2Source.class);
  private final Configuration sourceConfig;
  private final S2ContextDeserializationSchema<T> deserializationSchema;

  public S2Source(Configuration sourceConfig, DeserializationSchema<T> deserializationSchema) {
    this(sourceConfig, S2ContextDeserializationSchema.of(deserializationSchema));
  }

  public S2Source(
      Configuration sourceConfig, S2ContextDeserializationSchema<T> deserializationSchema) {
    this.sourceConfig = sourceConfig;
    this.deserializationSchema = deserializationSchema;
  }

  @Override
  public Boundedness getBoundedness() {
    return Boundedness.CONTINUOUS_UNBOUNDED;
  }

  @Override
  public SplitEnumerator<S2SourceSplit, S2EnumeratorState> createEnumerator(
      SplitEnumeratorContext<S2SourceSplit> enumContext) throws Exception {
    return S2SplitEnumerator.construct(enumContext, sourceConfig);
  }

  @Override
  public SplitEnumerator<S2SourceSplit, S2EnumeratorState> restoreEnumerator(
      SplitEnumeratorContext<S2SourceSplit> enumContext, S2EnumeratorState checkpoint)
      throws Exception {
    LOG.debug("restoreEnumerator");
    return S2SplitEnumerator.restoreFromSnapshot(
        enumContext,
        sourceConfig,
        checkpoint.streams,
        checkpoint.unassignedStreams,
        checkpoint.initialDistributionCompleted);
  }

  @Override
  public SimpleVersionedSerializer<S2SourceSplit> getSplitSerializer() {
    return new S2SplitSerializer();
  }

  @Override
  public SimpleVersionedSerializer<S2EnumeratorState> getEnumeratorCheckpointSerializer() {
    return new S2EnumeratorStateSerializer();
  }

  @Override
  public SourceReader<T, S2SourceSplit> createReader(SourceReaderContext readerContext)
      throws Exception {
    this.deserializationSchema.open(
        new DeserializationSchema.InitializationContext() {
          @Override
          public MetricGroup getMetricGroup() {
            return readerContext.metricGroup().addGroup("deserializer");
          }

          @Override
          public UserCodeClassLoader getUserCodeClassLoader() {
            return readerContext.getUserCodeClassLoader();
          }
        });

    return new S2SourceReader<T>(
        new SingleThreadFetcherManager<>(() -> new S2SplitReader(sourceConfig)),
        new S2RecordEmitter<>(this.deserializationSchema),
        sourceConfig,
        readerContext);
  }
}
