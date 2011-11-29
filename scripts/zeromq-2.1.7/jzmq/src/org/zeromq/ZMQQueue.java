package org.zeromq;

import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

/**
 * ZeroMQ Queue Device implementation.
 * 
 * @author Alois Belaska <alois.belaska@gmail.com>
 */
public class ZMQQueue implements Runnable {

    private final ZMQ.Poller poller;
    private final ZMQ.Socket inSocket;
    private final ZMQ.Socket outSocket;

    /**
     * Class constructor.
     * 
     * @param context
     *            a 0MQ context previously created.
     * @param inSocket
     *            input socket
     * @param outSocket
     *            output socket
     */
    public ZMQQueue(Context context, Socket inSocket, Socket outSocket) {
        this.inSocket = inSocket;
        this.outSocket = outSocket;

        this.poller = context.poller(2);
        this.poller.register(inSocket, ZMQ.Poller.POLLIN);
        this.poller.register(outSocket, ZMQ.Poller.POLLIN);
    }

    /**
     * Queuing of requests and replies.
     */
    @Override
	public void run() {
        byte[] msg = null;
        boolean more = true;

        while (!Thread.currentThread().isInterrupted()) {
            try {
                // wait while there are either requests or replies to process
                if (poller.poll(250000) < 1) {
                    continue;
                }

                // process a request
                if (poller.pollin(0)) {
                    more = true;
                    while (more) {
                        msg = inSocket.recv(0);

                        more = inSocket.hasReceiveMore();

                        if (msg != null) {
                            outSocket.send(msg, more ? ZMQ.SNDMORE : 0);
                        }
                    }
                }

                // process a reply
                if (poller.pollin(1)) {
                    more = true;
                    while (more) {
                        msg = outSocket.recv(0);

                        more = outSocket.hasReceiveMore();

                        if (msg != null) {
                            inSocket.send(msg, more ? ZMQ.SNDMORE : 0);
                        }
                    }
                }
            } catch (ZMQException e) {
                // context destroyed, exit
                if (ZMQ.Error.ETERM.getCode() == e.getErrorCode()) {
                    break;
                }
                throw e;
            }
        }
    }
}
