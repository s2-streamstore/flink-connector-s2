package s2.flink.sink;

import static s2.flink.config.S2SinkConfig.S2_SINK_BASIN;
import static s2.flink.config.S2SinkConfig.S2_SINK_STREAM;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.base.sink.writer.AsyncSinkWriter;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.base.sink.writer.config.AsyncSinkWriterConfiguration;
import s2.channel.BasinChannel;
import s2.channel.ManagedChannelFactory;
import s2.client.ManagedAppendSession;
import s2.client.StreamClient;
import s2.config.Config;
import s2.flink.config.S2ClientConfig;
import s2.types.AppendInput;
import s2.types.AppendOutput;
import s2.types.AppendRecord;

public class S2SinkWriter<InputT> extends AsyncSinkWriter<InputT, AppendRecord> {

  private final ManagedAppendSession managedAppendSession;
  private final ScheduledExecutorService executorService;
  private final Config s2Config;
  private final String basinName;
  private final BasinChannel basinChannel;
  private final StreamClient streamClient;

  public S2SinkWriter(
      ElementConverter<InputT, AppendRecord> elementConverter,
      WriterInitContext context,
      AsyncSinkWriterConfiguration configuration,
      Collection<BufferedRequestState<AppendRecord>> bufferedRequestStates,
      ReadableConfig clientConfiguration) {
    super(elementConverter, context, configuration, bufferedRequestStates);
    s2Config = S2ClientConfig.fromConfig(clientConfiguration);
    basinName = clientConfiguration.get(S2_SINK_BASIN);
    basinChannel = ManagedChannelFactory.forBasinOrStreamService(s2Config, basinName);
    executorService = Executors.newScheduledThreadPool(1);
    streamClient =
        StreamClient.newBuilder(
                this.s2Config,
                clientConfiguration.get(S2_SINK_BASIN),
                clientConfiguration.get(S2_SINK_STREAM))
            .withChannel(this.basinChannel)
            .withExecutor(this.executorService)
            .build();
    managedAppendSession = streamClient.managedAppendSession();
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
    this.streamClient.close();
    this.basinChannel.close();
    this.executorService.shutdown();
  }
}
