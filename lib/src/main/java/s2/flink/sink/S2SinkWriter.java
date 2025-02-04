package s2.flink.sink;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.connector.base.sink.writer.AsyncSinkWriter;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.base.sink.writer.config.AsyncSinkWriterConfiguration;
import s2.client.Client;
import s2.client.ManagedAppendSession;
import s2.client.StreamClient;
import s2.flink.properties.S2Config;
import s2.types.AppendInput;
import s2.types.AppendOutput;
import s2.types.AppendRecord;

public class S2SinkWriter<InputT> extends AsyncSinkWriter<InputT, AppendRecord> {

  private final StreamClient streamClient;
  private final ManagedAppendSession managedAppendSession;
  private final ExecutorService executorService;

  public S2SinkWriter(
      ElementConverter<InputT, AppendRecord> elementConverter,
      WriterInitContext context,
      AsyncSinkWriterConfiguration configuration,
      Collection<BufferedRequestState<AppendRecord>> bufferedRequestStates,
      Properties s2ConfigProperties,
      String basin,
      String stream) {
    super(elementConverter, context, configuration, bufferedRequestStates);
    streamClient = createStreamClient(basin, stream, s2ConfigProperties);
    managedAppendSession = streamClient.managedAppendSession();
    executorService = Executors.newCachedThreadPool();
  }

  private StreamClient createStreamClient(
      String basin, String stream, Properties s2ConfigProperties) {
    var config = S2Config.fromProperties(s2ConfigProperties);
    return new Client(config).basinClient(basin).streamClient(stream);
  }

  @Override
  protected void submitRequestEntries(
      List<AppendRecord> requestEntries, Consumer<List<AppendRecord>> requestToRetry) {
    try {
      var appendInput = AppendInput.newBuilder().withRecords(requestEntries).build();
      var fut = managedAppendSession.submit(appendInput, Duration.ofDays(1));
      Futures.addCallback(
          fut,
          new FutureCallback<>() {
            @Override
            public void onSuccess(AppendOutput result) {
              requestToRetry.accept(Collections.emptyList());
            }

            @Override
            public void onFailure(Throwable t) {
              // Never use this retry mechanism. S2 will retry the appends internally, if the client
              // has so configured.
              getFatalExceptionCons().accept(new RuntimeException(t));
            }
          },
          executorService);
    } catch (InterruptedException e) {
      getFatalExceptionCons().accept(new RuntimeException(e));
    }
  }

  @Override
  protected long getSizeInBytes(AppendRecord requestEntry) {
    return requestEntry.meteredBytes();
  }

  @Override
  public void close() {
    this.executorService.shutdown();
    try {
      this.streamClient.close();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    super.close();
  }
}
