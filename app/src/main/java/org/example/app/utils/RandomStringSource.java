package org.example.app.utils;

import java.util.Random;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class RandomStringSource extends RichParallelSourceFunction<String> {

  private final int endAfter;
  private volatile boolean running = true;
  private long count = 0;

  public RandomStringSource(int endAfter) {
    this.endAfter = endAfter;
  }

  @Override
  public void run(SourceContext<String> ctx) throws Exception {
    while (count < endAfter && running) {
      synchronized (ctx.getCheckpointLock()) {
        count++;
        String payload = RandomASCIIStringGenerator.generateRandomASCIIString(count + "=", 3);
        ctx.collect(payload);
      }
      // Thread.sleep(1000);
    }
  }

  @Override
  public void cancel() {
    running = false;
  }

  static class RandomASCIIStringGenerator {
    private static final String ASCII_PRINTABLE_CHARACTERS =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ" + "abcdefghijklmnopqrstuvwxyz" + "0123456789";

    private static final Random RANDOM = new Random();

    public static String generateRandomASCIIString(String prefix, int length) {
      if (length < 0) {
        throw new IllegalArgumentException("Length cannot be negative.");
      }

      StringBuilder sb = new StringBuilder(length);
      sb.append(prefix);
      for (int i = 0; i < length; i++) {
        int index = RANDOM.nextInt(ASCII_PRINTABLE_CHARACTERS.length());
        sb.append(ASCII_PRINTABLE_CHARACTERS.charAt(index));
      }
      return sb.toString();
    }
  }
}
