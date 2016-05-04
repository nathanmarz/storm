/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.security.auth;

import java.io.IOException;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.storm.utils.StormBoundedExponentialBackoffRetry;

public class TBackoffConnect {
    private static final Logger LOG = LoggerFactory.getLogger(TBackoffConnect.class);
    private int _completedRetries = 0;
    private int _retryTimes;
    private StormBoundedExponentialBackoffRetry waitGrabber;
    private boolean _retryForever = false;

    public TBackoffConnect(int retryTimes, int retryInterval, int retryIntervalCeiling, boolean retryForever) {

        _retryForever = retryForever;
        _retryTimes = retryTimes;
        waitGrabber = new StormBoundedExponentialBackoffRetry(retryInterval,
                                                              retryIntervalCeiling,
                                                              retryTimes);
    }

    public TBackoffConnect(int retryTimes, int retryInterval, int retryIntervalCeiling) {
        this(retryTimes, retryInterval, retryIntervalCeiling, false);
    }

    public TTransport doConnectWithRetry(ITransportPlugin transportPlugin, TTransport underlyingTransport, String host, String asUser) throws IOException {
        boolean connected = false;
        TTransport transportResult = null;
        while(!connected) {
            try {
                transportResult = transportPlugin.connect(underlyingTransport, host, asUser);
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
            int sleeptime = waitGrabber.getSleepTimeMs(_completedRetries, 0);

            LOG.debug("Failed to connect. Retrying... (" + Integer.toString( _completedRetries) + ") in " + Integer.toString(sleeptime) + "ms");

            Thread.sleep(sleeptime);
        } catch (InterruptedException e) {
            LOG.info("Nimbus connection retry interrupted.");
        }

        _completedRetries++;
    }

    private boolean canRetry() {
        return _retryForever || (_completedRetries < _retryTimes);
    }
}
