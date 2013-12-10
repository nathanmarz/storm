package backtype.storm.messaging.netty;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferOutputStream;
import org.jboss.netty.buffer.ChannelBuffers;

enum ControlMessage {
    CLOSE_MESSAGE((short)-100),
    EOB_MESSAGE((short)-201),
    OK_RESPONSE((short)-200),
    FAILURE_RESPONSE((short)-400);

    private short code;

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
        for(ControlMessage cm: ControlMessage.values()) {
          if(encoded == cm.code) return cm;
        }
        return null;
    }

    int encodeLength() {
        return 2; //short
    }
    
    /**
     * encode the current Control Message into a channel buffer
     * @throws Exception
     */
    ChannelBuffer buffer() throws Exception {
        ChannelBufferOutputStream bout = new ChannelBufferOutputStream(ChannelBuffers.directBuffer(encodeLength()));      
        write(bout);
        bout.close();
        return bout.buffer();
    }

    void write(ChannelBufferOutputStream bout) throws Exception {
        bout.writeShort(code);        
    } 
}
