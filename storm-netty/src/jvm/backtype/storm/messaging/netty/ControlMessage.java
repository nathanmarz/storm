package backtype.storm.messaging.netty;

class ControlMessage {
    static final short BASE_CODE = -100; 
    static final short OK = -200; //HTTP status: 200
    static final short EOB = -201; //end of a batch
    static final short FAILURE = -400; //HTTP status: 400 BAD REQUEST
    static final short CLOSE = -410; //HTTP status: 410 GONE
    
    static final ControlMessage CLOSE_MESSAGE = new ControlMessage(CLOSE);
    static final ControlMessage EOB_MESSAGE = new ControlMessage(EOB);
    static final ControlMessage OK_RESPONSE = new ControlMessage(OK);
    static final ControlMessage FAILURE_RESPONSE = new ControlMessage(FAILURE);
    
    private short code;
    
    ControlMessage() {
        code = OK;
    }
    
    ControlMessage(short code) {
        assert(code<BASE_CODE);
        this.code = code;
    }
    
    short code() {
        return code;
    }
    
    public String toString() {
        switch (code) {
        case OK: return "OK";
        case EOB: return "END_OF_BATCH";
        case FAILURE: return "FAILURE";
        case CLOSE: return "CLOSE";
        default: return "control message w/ code " + code;
        }
    }
    
    public boolean equals(Object obj) {
        if (obj == null) return false;
        if (obj instanceof ControlMessage)
            return ((ControlMessage)obj).code == code;
        return false;
    }
}
