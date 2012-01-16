package storm.dedup;

public interface DedupConstants {
  /**
   * dedup message stream
   */
  public static final String DEDUP_STREAM_ID = "_DEDUP_STREAM_ID_";
  
  /**
   * tuple id field
   */
  public static final String TUPLE_ID_FIELD = "_TUPLE_ID_";
  
  /**
   * state store name
   */
  public static final String STATE_STORE_NAME = "storm.dedup.state.store.name";
  
  public static final String TUPLE_ID_SEP = "_";
  public static final String TUPLE_ID_SUB_SEP = "-";
  
  public static final String COLON = ":";
}
