package backtype.storm.messaging.netty;

import backtype.storm.messaging.TaskMessage;

class Util {
    static final int OK = -200; //HTTP status: 200
    static final int EOB = -201; //end of a batch
    static final int FAILURE = -400; //HTTP status: 400 BAD REQUEST
    static final int CLOSE = -410; //HTTP status: 410 GONE
    
    static final TaskMessage CLOSE_MESSAGE = new TaskMessage(CLOSE, null);
    static final TaskMessage EOB_MESSAGE = new TaskMessage(EOB, null);
    static final TaskMessage OK_RESPONSE = new TaskMessage(OK, null);
    static final TaskMessage FAILURE_RESPONSE = new TaskMessage(FAILURE, null);
}
