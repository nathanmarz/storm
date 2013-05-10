package backtype.storm.messaging.netty;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferOutputStream;
import org.jboss.netty.buffer.ChannelBuffers;

class ControlMessage {
    private static final short CODE_CLOSE = -100;
    private static final short CODE_OK = -200;
    private static final short CODE_EOB = -201;
    private static final short CODE_FAILURE = -400;
    private short code;

    //request client/server to be closed
    private static final ControlMessage CLOSE_MESSAGE = new ControlMessage(CODE_CLOSE);
    //indicate the end of a batch request
    private static final ControlMessage EOB_MESSAGE = new ControlMessage(CODE_EOB);
    //success response
    private static final ControlMessage OK_RESPONSE = new ControlMessage(CODE_OK);
    //failre response
    private static final ControlMessage FAILURE_RESPONSE = new ControlMessage(CODE_FAILURE);

    static ControlMessage okResponse() {
        return OK_RESPONSE;
    }

    static ControlMessage failureResponse() {
        return FAILURE_RESPONSE;
    }

    static ControlMessage eobMessage() {
        return EOB_MESSAGE;
    }
    
    static ControlMessage closeMessage() {
        return CLOSE_MESSAGE;
    }

    //private constructor
    private ControlMessage(short code) {
        this.code = code;
    }

    /**
     * Return a control message per an encoded status code
     * @param encoded
     * @return
     */
    static ControlMessage mkMessage(short encoded) {
        switch (encoded) {
        case CODE_OK: return OK_RESPONSE;
        case CODE_EOB: return EOB_MESSAGE;
        case CODE_FAILURE: return FAILURE_RESPONSE;
        case CODE_CLOSE: return CLOSE_MESSAGE;
        }

        return null;
    }

    /**
     * encode the current Control Message into a channel buffer
     * @param bout
     * @throws Exception
     */
    ChannelBuffer buffer() throws Exception {
        ChannelBufferOutputStream bout = new ChannelBufferOutputStream(ChannelBuffers.dynamicBuffer());      
        write(bout);
        bout.close();
        return bout.buffer();
    }

    void write(ChannelBufferOutputStream bout) throws Exception {
        bout.writeShort(code);        
    }
    
    /**
     * comparison 
     */
    public boolean equals(Object obj) {
        if (obj == null) return false;
        if (obj instanceof ControlMessage)
            return ((ControlMessage)obj).code == code;
        return false;
    }

    /**
     * human readable string
     */
    public String toString() {
        switch (code) {
        case CODE_OK: return "ControlMessage OK";
        case CODE_EOB: return "ControlMessage END_OF_BATCH";
        case CODE_FAILURE: return "ControlMessage FAILURE";
        case CODE_CLOSE: return "ControlMessage CLOSE";
        }
        return null;
    }   
}
