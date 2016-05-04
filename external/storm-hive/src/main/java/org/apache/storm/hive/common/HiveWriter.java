/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.hive.common;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.hcatalog.streaming.*;
import org.apache.storm.hive.bolt.mapper.HiveMapper;
import org.apache.storm.tuple.Tuple;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class HiveWriter {

    private static final Logger LOG = LoggerFactory
        .getLogger(HiveWriter.class);

    private final HiveEndPoint endPoint;
    private final StreamingConnection connection;
    private final int txnsPerBatch;
    private final RecordWriter recordWriter;
    private final ExecutorService callTimeoutPool;
    private final long callTimeout;
    private final Object txnBatchLock = new Object();
    private TransactionBatch txnBatch;
    private long lastUsed; // time of last flush on this writer
    protected boolean closed; // flag indicating HiveWriter was closed
    private boolean autoCreatePartitions;
    private UserGroupInformation ugi;
    private int totalRecords = 0;

    public HiveWriter(HiveEndPoint endPoint, int txnsPerBatch,
                      boolean autoCreatePartitions, long callTimeout,
                      ExecutorService callTimeoutPool, HiveMapper mapper, UserGroupInformation ugi)
        throws InterruptedException, ConnectFailure {
        try {
            this.autoCreatePartitions = autoCreatePartitions;
            this.callTimeout = callTimeout;
            this.callTimeoutPool = callTimeoutPool;
            this.endPoint = endPoint;
            this.ugi = ugi;
            this.connection = newConnection(ugi);
            this.txnsPerBatch = txnsPerBatch;
            this.recordWriter = mapper.createRecordWriter(endPoint);
            this.txnBatch = nextTxnBatch(recordWriter);
            this.closed = false;
            this.lastUsed = System.currentTimeMillis();
        } catch(InterruptedException e) {
            throw e;
        } catch(RuntimeException e) {
            throw e;
        } catch(Exception e) {
            throw new ConnectFailure(endPoint, e);
        }
    }

    @Override
    public String toString() {
          return "{ "
              + "endPoint = " + endPoint.toString()
              + ", TransactionBatch = " + txnBatch.toString() + " }";
    }

    /**
     * Write data <br />
     *
     * @throws IOException
     * @throws InterruptedException
     */
    public synchronized void write(final byte[] record)
        throws WriteFailure, SerializationError, InterruptedException {
        if (closed) {
            throw new IllegalStateException("This hive streaming writer was closed " +
                                            "and thus no longer able to write : " + endPoint);
        }
        // write the tuple
        try {
            LOG.debug("Writing event to {}", endPoint);
            callWithTimeout(new CallRunner<Void>() {
                    @Override
                    public Void call() throws StreamingException, InterruptedException {
                        txnBatch.write(record);
                        totalRecords++;
                        return null;
                    }
                });
        } catch(SerializationError se) {
            throw new SerializationError(endPoint.toString() + " SerializationError", se);
        } catch(StreamingException e) {
            throw new WriteFailure(endPoint, txnBatch.getCurrentTxnId(), e);
        } catch(TimeoutException e) {
            throw new WriteFailure(endPoint, txnBatch.getCurrentTxnId(), e);
        }
    }

    /**
     * Commits the current Txn if totalRecordsPerTransaction > 0 .
     * If 'rollToNext' is true, will switch to next Txn in batch or to a
     *       new TxnBatch if current Txn batch is exhausted
     */
    public void flush(boolean rollToNext)
        throws CommitFailure, TxnBatchFailure, TxnFailure, InterruptedException {
        // if there are no records do not call flush
        if (totalRecords <= 0) return;
        try {
            synchronized(txnBatchLock) {
                commitTxn();
                nextTxn(rollToNext);
                totalRecords = 0;
                lastUsed = System.currentTimeMillis();
            }
        } catch(StreamingException e) {
            throw new TxnFailure(txnBatch, e);
        }
    }

    /** Queues up a heartbeat request on the current and remaining txns using the
     *  heartbeatThdPool and returns immediately
     */
    public void heartBeat() throws InterruptedException {
        // 1) schedule the heartbeat on one thread in pool
        synchronized(txnBatchLock) {
            try {
                callWithTimeout(new CallRunner<Void>() {
                        @Override
                        public Void call() throws Exception {
                            try {
                                LOG.info("Sending heartbeat on batch " + txnBatch);
                                txnBatch.heartbeat();
                            } catch (StreamingException e) {
                                LOG.warn("Heartbeat error on batch " + txnBatch, e);
                            }
                            return null;
                        }
                    });
            } catch (InterruptedException e) {
                throw e;
            } catch (Exception e) {
                LOG.warn("Unable to send heartbeat on Txn Batch " + txnBatch,  e);
                // Suppressing exceptions as we don't care for errors on heartbeats
            }
        }
    }

    /**
     * returns totalRecords written so far in a transaction
     * @returns totalRecords
     */
    public int getTotalRecords() {
        return totalRecords;
    }

    /**
     * Flush and Close current transactionBatch.
     */
    public void flushAndClose() throws TxnBatchFailure, TxnFailure, CommitFailure,
            IOException, InterruptedException {
        flush(false);
        close();
    }
    /**
     * Close the Transaction Batch and connection
     * @throws IOException
     * @throws InterruptedException
     */
    public void close() throws IOException, InterruptedException {
        closeTxnBatch();
        closeConnection();
        closed = true;
    }

    private void closeConnection() throws InterruptedException {
        LOG.info("Closing connection to end point : {}", endPoint);
        try {
            callWithTimeout(new CallRunner<Void>() {
                    @Override
                    public Void call() throws Exception {
                        connection.close(); // could block
                        return null;
                    }
                });
        } catch(Exception e) {
            LOG.warn("Error closing connection to EndPoint : " + endPoint, e);
            // Suppressing exceptions as we don't care for errors on connection close
        }
    }

    private void commitTxn() throws CommitFailure, InterruptedException {
        LOG.debug("Committing Txn id {} to {}", txnBatch.getCurrentTxnId() , endPoint);
        try {
            callWithTimeout(new CallRunner<Void>() {
                    @Override
                    public Void call() throws Exception {
                        txnBatch.commit(); // could block
                        return null;
                    }
                });
        } catch (StreamingException e) {
            throw new CommitFailure(endPoint, txnBatch.getCurrentTxnId(), e);
        } catch (TimeoutException e) {
            throw new CommitFailure(endPoint, txnBatch.getCurrentTxnId(), e);
        }
    }

    private StreamingConnection newConnection(final UserGroupInformation ugi)
        throws InterruptedException, ConnectFailure {
        try {
            return  callWithTimeout(new CallRunner<StreamingConnection>() {
                    @Override
                    public StreamingConnection call() throws Exception {
                        return endPoint.newConnection(autoCreatePartitions, null, ugi); // could block
                    }
                });
        } catch(StreamingException e) {
            throw new ConnectFailure(endPoint, e);
        } catch(TimeoutException e) {
            throw new ConnectFailure(endPoint, e);
        }
    }

    private TransactionBatch nextTxnBatch(final RecordWriter recordWriter)
        throws InterruptedException, TxnBatchFailure {
        LOG.debug("Fetching new Txn Batch for {}", endPoint);
        TransactionBatch batch = null;
        try {
            batch = callWithTimeout(new CallRunner<TransactionBatch>() {
                @Override
                public TransactionBatch call() throws Exception {
                    return connection.fetchTransactionBatch(txnsPerBatch, recordWriter); // could block
                }
            });
            batch.beginNextTransaction();
            LOG.debug("Acquired {}. Switching to first txn", batch);
        } catch(TimeoutException e) {
            throw new TxnBatchFailure(endPoint, e);
        } catch(StreamingException e) {
            throw new TxnBatchFailure(endPoint, e);
        }
        return batch;
    }

    private void closeTxnBatch() throws  InterruptedException {
        try {
            LOG.debug("Closing Txn Batch {}", txnBatch);
            callWithTimeout(new CallRunner<Void>() {
                    @Override
                        public Void call() throws Exception {
                        if(txnBatch != null) {
                            txnBatch.close(); // could block
                        }
                        return null;
                    }
                });
        } catch(InterruptedException e) {
            throw e;
        } catch(Exception e) {
            LOG.warn("Error closing txn batch "+ txnBatch, e);
        }
    }

    /**
     * Aborts the current Txn and switches to next Txn.
     * @throws StreamingException if could not get new Transaction Batch, or switch to next Txn
     */
    public void abort() throws StreamingException, TxnBatchFailure, InterruptedException {
        synchronized(txnBatchLock) {
            abortTxn();
            nextTxn(true); // roll to next
        }
    }


    /**
     * Aborts current Txn in the txnBatch.
     */
    private void abortTxn() throws InterruptedException {
        LOG.info("Aborting Txn id {} on End Point {}", txnBatch.getCurrentTxnId(), endPoint);
        try {
            callWithTimeout(new CallRunner<Void>() {
                    @Override
                        public Void call() throws StreamingException, InterruptedException {
                        txnBatch.abort(); // could block
                        return null;
                    }
                });
        } catch (InterruptedException e) {
            throw e;
        } catch (TimeoutException e) {
            LOG.warn("Timeout while aborting Txn " + txnBatch.getCurrentTxnId() + " on EndPoint: " + endPoint, e);
        } catch (Exception e) {
            LOG.warn("Error aborting Txn " + txnBatch.getCurrentTxnId() + " on EndPoint: " + endPoint, e);
            // Suppressing exceptions as we don't care for errors on abort
        }
    }


    /**
     * if there are remainingTransactions in current txnBatch, begins nextTransactions
     * otherwise creates new txnBatch.
     * @param boolean rollToNext
     */
    private void nextTxn(boolean rollToNext) throws StreamingException, InterruptedException, TxnBatchFailure {
        if(txnBatch.remainingTransactions() == 0) {
            closeTxnBatch();
            txnBatch = null;
            if(rollToNext) {
                txnBatch = nextTxnBatch(recordWriter);
            }
        } else if(rollToNext) {
            LOG.debug("Switching to next Txn for {}", endPoint);
            txnBatch.beginNextTransaction(); // does not block
        }
    }

    /**
     * If the current thread has been interrupted, then throws an
     * exception.
     * @throws InterruptedException
     */
    private static void checkAndThrowInterruptedException()
        throws InterruptedException {
        if (Thread.currentThread().interrupted()) {
            throw new InterruptedException("Timed out before Hive call was made. "
                                           + "Your callTimeout might be set too low or Hive calls are "
                                           + "taking too long.");
        }
    }

    /**
     * Execute the callable on a separate thread and wait for the completion
     * for the specified amount of time in milliseconds. In case of timeout
     * cancel the callable and throw an IOException
     */
    private <T> T callWithTimeout(final CallRunner<T> callRunner)
        throws TimeoutException, StreamingException, InterruptedException {
        Future<T> future = callTimeoutPool.submit(new Callable<T>() {
                @Override
                public T call() throws Exception {
                    return callRunner.call();
                }
            });
        try {
            if (callTimeout > 0) {
                return future.get(callTimeout, TimeUnit.MILLISECONDS);
            } else {
                return future.get();
            }
        } catch (TimeoutException eT) {
            future.cancel(true);
            throw eT;
        } catch (ExecutionException e1) {
            Throwable cause = e1.getCause();
            if (cause instanceof IOException) {
                throw new StreamingIOFailure("I/O Failure", (IOException) cause);
            } else if (cause instanceof StreamingException) {
                throw (StreamingException) cause;
            } else if (cause instanceof InterruptedException) {
                throw (InterruptedException) cause;
            } else if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            } else if (cause instanceof TimeoutException) {
                throw new StreamingException("Operation Timed Out.", (TimeoutException) cause);
            } else {
                throw new RuntimeException(e1);
            }
        }
    }

    public long getLastUsed() {
        return lastUsed;
    }

    private byte[] generateRecord(Tuple tuple) {
        StringBuilder buf = new StringBuilder();
        for (Object o: tuple.getValues()) {
            buf.append(o);
            buf.append(",");
        }
        return buf.toString().getBytes();
    }

    /**
     * Simple interface whose <tt>call</tt> method is called by
     * {#callWithTimeout} in a new thread inside a
     * {@linkplain java.security.PrivilegedExceptionAction#run()} call.
     * @param <T>
     */
    private interface CallRunner<T> {
        T call() throws Exception;
    }

    public static class Failure extends Exception {
        public Failure(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public static class WriteFailure extends Failure {
        public WriteFailure(HiveEndPoint endPoint, Long currentTxnId, Throwable cause) {
            super("Failed writing to : " + endPoint + ". TxnID : " + currentTxnId, cause);
        }
    }

    public static class CommitFailure extends Failure {
        public CommitFailure(HiveEndPoint endPoint, Long txnID, Throwable cause) {
            super("Commit of Txn " + txnID + " failed on EndPoint: " + endPoint, cause);
        }
    }

    public static class ConnectFailure extends Failure {
        public ConnectFailure(HiveEndPoint ep, Throwable cause) {
            super("Failed connecting to EndPoint " + ep, cause);
        }
    }

    public static class TxnBatchFailure extends Failure {
        public TxnBatchFailure(HiveEndPoint ep, Throwable cause) {
            super("Failed acquiring Transaction Batch from EndPoint: " + ep, cause);
        }
    }

    public static class TxnFailure extends Failure {
        public TxnFailure(TransactionBatch txnBatch, Throwable cause) {
            super("Failed switching to next Txn in TxnBatch " + txnBatch, cause);
        }
    }
}
