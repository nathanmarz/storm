package backtype.storm.multilang;

/**
 * SpoutMsg is an object that represents the data sent from a shell spout to a
 * process that implements a multi-language spout. The SpoutMsg is used to send
 * a "next", "ack" or "fail" message to a spout.
 *
 * <p>
 * Spout messages are objects sent to the ISerializer interface, for
 * serialization according to the wire protocol implemented by the serializer.
 * The SpoutMsg class allows for a decoupling between the serialized
 * representation of the data and the data itself.
 * </p>
 */
public class SpoutMsg {
    private String command;
    private Object id;

    public String getCommand() {
        return command;
    }

    public void setCommand(String command) {
        this.command = command;
    }

    public Object getId() {
        return id;
    }

    public void setId(Object id) {
        this.id = id;
    }
}
