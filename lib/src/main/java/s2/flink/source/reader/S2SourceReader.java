package s2.flink.source.reader;

import java.util.Map;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.SourceReaderBase;
import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcherManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import s2.flink.source.split.S2SourceSplit;
import s2.flink.source.split.S2SourceSplitState;
import s2.types.SequencedRecord;

public class S2SourceReader<T>
    extends SourceReaderBase<SequencedRecord, T, S2SourceSplit, S2SourceSplitState> {

  private static final Logger LOG = LoggerFactory.getLogger(S2SourceReader.class);

  public S2SourceReader(
      SplitFetcherManager<SequencedRecord, S2SourceSplit> splitFetcherManager,
      RecordEmitter<SequencedRecord, T, S2SourceSplitState> recordEmitter,
      Configuration config,
      SourceReaderContext context) {
    super(splitFetcherManager, recordEmitter, config, context);
  }

  @Override
  protected void onSplitFinished(Map<String, S2SourceSplitState> finishedSplitIds) {
    LOG.error("splits finished {}", finishedSplitIds);
    throw new UnsupportedOperationException("Splits are not expected to finish.");
  }

  @Override
  protected S2SourceSplitState initializedState(S2SourceSplit split) {
    LOG.debug("split state initialized for split {}", split.splitId());
    return new S2SourceSplitState(split);
  }

  @Override
  protected S2SourceSplit toSplitType(String splitId, S2SourceSplitState splitState) {
    return splitState.toSplit();
  }
}
