package org.example.app.eventstream;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.IntStream;
import org.apache.flink.util.Preconditions;
import s2.channel.ManagedChannelFactory;
import s2.client.ManagedAppendSession;
import s2.client.StreamClient;
import s2.config.AppendRetryPolicy;
import s2.config.Config;
import s2.config.Endpoints;
import s2.types.AppendInput;
import s2.types.AppendOutput;
import s2.types.AppendRecord;

public class EventSpoofer {

  static final String[] QUERIES = {
    "mid-century modern",
    "post-modern",
    "memphis milano",
    "home and living",
    "kitchen",
    "linen",
    "gifts",
    "birthday"
  };

  static class RandomEvent implements Iterator<String> {

    final Random random = new Random();

    @Override
    public boolean hasNext() {
      return true;
    }

    @Override
    public String next() {
      final var userId = random.nextInt(20);
      if (random.nextDouble() < 0.2) {
        final var query = QUERIES[random.nextInt(QUERIES.length)];
        return String.format("user=%s;search=%s", userId, query);
      } else {
        final var itemId = random.nextInt(20);
        if (random.nextDouble() < 0.2) {
          return String.format("user=%s;item=%s;action=view", userId, itemId);
        } else {
          return String.format("user=%s;item=%s;action=cart", userId, itemId);
        }
      }
    }
  }

  static class ConvertingJourney implements Iterator<String> {

    final int userId;
    final String query;
    final int itemIdToBuy;

    // query => 0
    // view => 1
    // cart => 2
    // buy => 3
    // finished => 4
    int stage = 0;

    ConvertingJourney(int userId, String query, int itemIdToBuy) {
      this.userId = userId;
      this.query = query;
      this.itemIdToBuy = itemIdToBuy;
    }

    @Override
    public boolean hasNext() {
      return stage < 4;
    }

    @Override
    public String next() {
      stage++;
      switch (stage) {
        case 1 -> {
          return String.format("user=%s;search=%s", userId, query);
        }
        case 2 -> {
          return String.format("user=%s;item=%s;action=view", userId, itemIdToBuy);
        }
        case 3 -> {
          return String.format("user=%s;item=%s;action=cart", userId, itemIdToBuy);
        }
        case 4 -> {
          return String.format("user=%s;item=%s;action=buy", userId, itemIdToBuy);
        }
      }
      return "";
    }
  }

  public static void main(String[] args) throws InterruptedException, ExecutionException {

    final var authToken = Preconditions.checkNotNull(System.getenv("S2_AUTH_TOKEN"));
    final var basinName = Preconditions.checkNotNull(System.getenv("MY_BASIN"));

    final var config =
        Config.newBuilder(authToken)
            .withEndpoints(Endpoints.fromEnvironment())
            .withMaxAppendInflightBytes(1024 * 1024 * 5)
            .withAppendRetryPolicy(AppendRetryPolicy.NO_SIDE_EFFECTS)
            .build();

    final LinkedBlockingQueue<ListenableFuture<AppendOutput>> pendingAppends =
        new LinkedBlockingQueue<>();

    try (final var executor =
            MoreExecutors.listeningDecorator(Executors.newScheduledThreadPool(12));
        final var channel = ManagedChannelFactory.forBasinOrStreamService(config, basinName)) {

      final var consumer =
          executor.submit(
              () -> {
                try {
                  while (true) {
                    var output = pendingAppends.take().get();
                    System.out.println(output);
                    if (output == null) {
                      System.out.println("No more appends");
                      break;
                    }
                  }
                } catch (Exception e) {
                  System.out.println("Error during append");
                }
              });

      final List<ManagedAppendSession> appendSessions =
          IntStream.range(0, 10)
              .mapToObj(i -> String.format("host/000%s", i))
              .map(
                  streamName ->
                      StreamClient.newBuilder(config, basinName, streamName)
                          .withChannel(channel)
                          .withExecutor(executor)
                          .build()
                          .managedAppendSession())
              .toList();

      final var random = new Random();

      // itemId -> convertingQuery
      final HashMap<Integer, List<String>> stats = new HashMap<>();
      var randomEvent = new RandomEvent();
      var activeJourney =
          new ConvertingJourney(
              random.nextInt(20), QUERIES[random.nextInt(QUERIES.length)], random.nextInt(20));
      for (int i = 0; i < 1000; i++) {
        // Pick a session at random.
        final var sessionIndex = random.nextInt(appendSessions.size());
        System.out.println("index: " + sessionIndex);
        final var session = appendSessions.get(sessionIndex);

        String event;
        if (random.nextDouble() < 0.2) {
          event = activeJourney.next();
          System.out.println("event: " + event);
          if (!activeJourney.hasNext()) {
            stats.putIfAbsent(activeJourney.itemIdToBuy, new ArrayList<>());
            stats.get(activeJourney.itemIdToBuy).add(activeJourney.query);

            activeJourney =
                new ConvertingJourney(
                    random.nextInt(20),
                    QUERIES[random.nextInt(QUERIES.length)],
                    random.nextInt(20));
            ;
          }
        } else {
          event = randomEvent.next();
        }

        var fut =
            session.submit(
                AppendInput.newBuilder()
                    .withRecords(
                        List.of(
                            AppendRecord.newBuilder()
                                .withBody(ByteString.copyFromUtf8(event))
                                .build()))
                    .build(),
                Duration.ofSeconds(10));
        pendingAppends.put(fut);
      }

      pendingAppends.put(Futures.immediateFuture(null));

      stats.forEach(
          (k, v) -> {
            System.out.printf("item=%s, queries=%s%n", k, v);
          });

      // Await responses for all.
      consumer.get();

      appendSessions.forEach(ManagedAppendSession::close);
    }
  }
}
