package s2.flink.source.serialization;

public class S2Context<T> {
  public final String stream;
  public final Long seqNum;
  public final T elem;

  S2Context(String stream, Long seqNum, T elem) {
    this.stream = stream;
    this.seqNum = seqNum;
    this.elem = elem;
  }

  @Override
  public String toString() {
    return stream + "." + seqNum + " => " + elem;
  }
}
