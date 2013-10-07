package backtype.storm.multilang;

/**
 * A NoOutputException states that no data has been received from the connected
 * non-JVM process.
 */
public class NoOutputException extends Exception {
	public NoOutputException() { super(); }
	public NoOutputException(String message) { super(message); }
	public NoOutputException(String message, Throwable cause) { super(message, cause); }
	public NoOutputException(Throwable cause) { super(cause); }
}