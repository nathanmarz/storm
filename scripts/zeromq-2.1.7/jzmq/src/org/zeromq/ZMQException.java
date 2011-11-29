package org.zeromq;

/**
 * ZeroMQ runtime exception.
 * 
 * @author Alois Belaska <alois.belaska@gmail.com>
 */
public class ZMQException extends RuntimeException {
    private static final long serialVersionUID = -978820750094924644L;

    private int errorCode = 0;

    public ZMQException(String message, int errorCode) {
        super(message);
        this.errorCode = errorCode;
    }

    /**
     * @return error code
     */
    public int getErrorCode() {
        return errorCode;
    }

    @Override
	public String toString() {
        return super.toString() + "(0x" + Integer.toHexString(errorCode) + ")";
    }
}
