package backtype.storm.messaging;

public abstract class ConnectionWithStatus implements IConnection {

  public static enum Status {

    /**
     * we are establishing a active connection with target host. The new data
     * sending request can be buffered for future sending, or dropped(cases like
     * there is no enough memory). It varies with difference IConnection
     * implementations.
     */
    Connecting,

    /**
     * We have a alive connection channel, which can be used to transfer data.
     */
    Ready,

    /**
     * The connection channel is closed or being closed. We don't accept further
     * data sending or receiving. All data sending request will be dropped.
     */
    Closed
  };

  /**
   * whether this connection is available to transfer data
   */
  public abstract Status status();

}