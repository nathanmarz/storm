package backtype.storm.security.auth;

import java.io.IOException;
import java.util.Random;
import org.apache.thrift7.transport.TTransport;
import org.apache.thrift7.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import backtype.storm.utils.Utils.BoundedExponentialBackoffRetry;

public class TBackoffConnect {
    private static final Logger LOG = LoggerFactory.getLogger(TBackoffConnect.class);
    private int completedRetries = 0;
    private int retryTimes;
    private BoundedExponentialBackoffRetry waitGrabber;

    public TBackoffConnect(int retryTimes, int retryInterval, int retryIntervalCeiling) {

        this.retryTimes = retryTimes;
        waitGrabber = new BoundedExponentialBackoffRetry(retryInterval,
                                                         retryTimes,
                                                         retryIntervalCeiling);
    }

    public TTransport doConnectWithRetry(ITransportPlugin transportPlugin, TTransport underlyingTransport, String host) throws IOException {
        boolean connected = false;
        TTransport transportResult = null;
        while(!connected) {
            try {
                transportResult = transportPlugin.connect(underlyingTransport, host);
                connected = true;
            } catch (TTransportException ex) {
                retryNext(ex);
            }
        }
        return transportResult;
    }

    private void retryNext(TTransportException ex) {
        if(!canRetry()) {
            throw new RuntimeException(ex);
        }
        try {
            int sleeptime = waitGrabber.getSleepTimeMs(completedRetries, 0);

            LOG.debug("Failed to connect. Retrying... (" + Integer.toString( completedRetries) + ") in " + Integer.toString(sleeptime) + "ms");

            Thread.sleep(sleeptime);
        } catch (InterruptedException e) {
            LOG.info("Nimbus connection retry interrupted.");
        }

        completedRetries++;
    }

    private boolean canRetry() {
        return (completedRetries < retryTimes);
    }
}