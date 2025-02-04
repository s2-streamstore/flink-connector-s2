package s2.flink.sink;

public class S2AsyncSinkConfig {
  public static final int MAX_BATCH_COUNT = 1000;
  public static final int MAX_BATCH_SIZE_BYTES = 1024 * 1024;
  public static final int MAX_RECORD_SIZE_BYTES = MAX_BATCH_SIZE_BYTES;
  public static final long DEFAULT_MAX_TIME_IN_BUFFER_MS = 1000L;
  public static final int DEFAULT_MAX_BUFFERED_REQUESTS = 2000;
  public static final int DEFAULT_MAX_INFLIGHT_REQUESTS = 500;
}
