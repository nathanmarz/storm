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
package org.apache.storm.blobstore;

import org.apache.storm.Config;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.BeginDownloadResult;
import org.apache.storm.generated.ListBlobsResult;
import org.apache.storm.generated.ReadableBlobMeta;
import org.apache.storm.generated.SettableBlobMeta;
import org.apache.storm.generated.KeyAlreadyExistsException;
import org.apache.storm.generated.KeyNotFoundException;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * NimbusBlobStore is a USER facing client API to perform
 * basic operations such as create, update, delete and read
 * for local and hdfs blob store.
 *
 * For local blob store it is also the client facing API for
 * supervisor in order to download blobs from nimbus.
 */
public class NimbusBlobStore extends ClientBlobStore {
    private static final Logger LOG = LoggerFactory.getLogger(NimbusBlobStore.class);

    public class NimbusKeyIterator implements Iterator<String> {
        private ListBlobsResult listBlobs = null;
        private int offset = 0;
        private boolean eof = false;

        public NimbusKeyIterator(ListBlobsResult listBlobs) {
            this.listBlobs = listBlobs;
            this.eof = (listBlobs.get_keys_size() == 0);
        }

        private boolean isCacheEmpty() {
            return listBlobs.get_keys_size() <= offset;
        }

        private void readMore() throws TException {
            if (!eof) {
                offset = 0;
                synchronized(client) {
                    listBlobs = client.getClient().listBlobs(listBlobs.get_session());
                }
                if (listBlobs.get_keys_size() == 0) {
                    eof = true;
                }
            }
        }

        @Override
        public synchronized boolean hasNext() {
            try {
                if (isCacheEmpty()) {
                    readMore();
                }
            } catch (TException e) {
                throw new RuntimeException(e);
            }
            return !eof;
        }

        @Override
        public synchronized String next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            String ret = listBlobs.get_keys().get(offset);
            offset++;
            return ret;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Delete Not Supported");
        }
    }

    public class NimbusDownloadInputStream extends InputStreamWithMeta {
        private BeginDownloadResult beginBlobDownload;
        private byte[] buffer = null;
        private int offset = 0;
        private int end = 0;
        private boolean eof = false;

        public NimbusDownloadInputStream(BeginDownloadResult beginBlobDownload) {
            this.beginBlobDownload = beginBlobDownload;
        }

        @Override
        public long getVersion() throws IOException {
            return beginBlobDownload.get_version();
        }

        @Override
        public synchronized int read() throws IOException {
            try {
                if (isEmpty()) {
                    readMore();
                    if (eof) {
                        return -1;
                    }
                }
                int length = Math.min(1, available());
                if (length == 0) {
                    return -1;
                }
                int ret = buffer[offset];
                offset += length;
                return ret;
            } catch(TException exp) {
                throw new IOException(exp);
            }
        }

        @Override
        public synchronized int read(byte[] b, int off, int len) throws IOException {
            try {
                if (isEmpty()) {
                    readMore();
                    if (eof) {
                        return -1;
                    }
                }
                int length = Math.min(len, available());
                System.arraycopy(buffer, offset, b, off, length);
                offset += length;
                return length;
            } catch(TException exp) {
                throw new IOException(exp);
            }
        }

        private boolean isEmpty() {
            return buffer == null || offset >= end;
        }

        private void readMore() throws TException {
            if (!eof) {
                ByteBuffer buff;
                synchronized(client) {
                    buff = client.getClient().downloadBlobChunk(beginBlobDownload.get_session());
                }
                buffer = buff.array();
                offset = buff.arrayOffset() + buff.position();
                int length = buff.remaining();
                end = offset + length;
                if (length == 0) {
                    eof = true;
                }
            }
        }

        @Override
        public synchronized int read(byte[] b) throws IOException {
            return read(b, 0, b.length);
        }

        @Override
        public synchronized int available() {
            return buffer == null ? 0 : (end - offset);
        }

        @Override
        public long getFileLength() {
            return beginBlobDownload.get_data_size();
        }
    }

    public class NimbusUploadAtomicOutputStream extends AtomicOutputStream {
        private String session;
        private int maxChunkSize = 4096;
        private String key;

        public NimbusUploadAtomicOutputStream(String session, int bufferSize, String key) {
            this.session = session;
            this.maxChunkSize = bufferSize;
            this.key = key;
        }

        @Override
        public void cancel() throws IOException {
            try {
                synchronized(client) {
                    client.getClient().cancelBlobUpload(session);
                }
            } catch (TException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void write(int b) throws IOException {
            try {
                synchronized(client) {
                    client.getClient().uploadBlobChunk(session, ByteBuffer.wrap(new byte[] {(byte)b}));
                }
            } catch (TException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void write(byte []b) throws IOException {
            write(b, 0, b.length);
        }

        @Override
        public void write(byte []b, int offset, int len) throws IOException {
            try {
                int end = offset + len;
                for (int realOffset = offset; realOffset < end; realOffset += maxChunkSize) {
                    int realLen = Math.min(end - realOffset, maxChunkSize);
                    LOG.debug("Writing {} bytes of {} remaining",realLen,(end-realOffset));
                    synchronized(client) {
                        client.getClient().uploadBlobChunk(session, ByteBuffer.wrap(b, realOffset, realLen));
                    }
                }
            } catch (TException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void close() throws IOException {
            try {
                synchronized(client) {
                    client.getClient().finishBlobUpload(session);
                    client.getClient().createStateInZookeeper(key);
                }
            } catch (TException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private NimbusClient client;
    private int bufferSize = 4096;

    @Override
    public void prepare(Map conf) {
        this.client = NimbusClient.getConfiguredClient(conf);
        if (conf != null) {
            this.bufferSize = Utils.getInt(conf.get(Config.STORM_BLOBSTORE_INPUTSTREAM_BUFFER_SIZE_BYTES), bufferSize);
        }
    }

    @Override
    protected AtomicOutputStream createBlobToExtend(String key, SettableBlobMeta meta)
            throws AuthorizationException, KeyAlreadyExistsException {
        try {
            synchronized(client) {
                return new NimbusUploadAtomicOutputStream(client.getClient().beginCreateBlob(key, meta), this.bufferSize, key);
            }
        } catch (AuthorizationException | KeyAlreadyExistsException exp) {
            throw exp;
        } catch (TException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public AtomicOutputStream updateBlob(String key)
            throws AuthorizationException, KeyNotFoundException {
        try {
            synchronized(client) {
                return new NimbusUploadAtomicOutputStream(client.getClient().beginUpdateBlob(key), this.bufferSize, key);
            }
        } catch (AuthorizationException | KeyNotFoundException exp) {
            throw exp;
        } catch (TException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ReadableBlobMeta getBlobMeta(String key) throws AuthorizationException, KeyNotFoundException {
        try {
            synchronized(client) {
                return client.getClient().getBlobMeta(key);
            }
        } catch (AuthorizationException | KeyNotFoundException exp) {
            throw exp;
        } catch (TException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void setBlobMetaToExtend(String key, SettableBlobMeta meta)
            throws AuthorizationException, KeyNotFoundException {
        try {
            synchronized(client) {
                client.getClient().setBlobMeta(key, meta);
            }
        } catch (AuthorizationException | KeyNotFoundException exp) {
            throw exp;
        } catch (TException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void deleteBlob(String key) throws AuthorizationException, KeyNotFoundException {
        try {
            synchronized(client) {
                client.getClient().deleteBlob(key);
            }
        } catch (AuthorizationException | KeyNotFoundException exp) {
            throw exp;
        } catch (TException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void createStateInZookeeper(String key) {
        try {
            synchronized(client) {
                client.getClient().createStateInZookeeper(key);
            }
        } catch (TException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public InputStreamWithMeta getBlob(String key) throws AuthorizationException, KeyNotFoundException {
        try {
            synchronized(client) {
                return new NimbusDownloadInputStream(client.getClient().beginBlobDownload(key));
            }
        } catch (AuthorizationException | KeyNotFoundException exp) {
            throw exp;
        } catch (TException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Iterator<String> listKeys() {
        try {
            synchronized(client) {
                return new NimbusKeyIterator(client.getClient().listBlobs(""));
            }
        } catch (TException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int getBlobReplication(String key) throws AuthorizationException, KeyNotFoundException {
        try {
            return client.getClient().getBlobReplication(key);
        } catch (AuthorizationException | KeyNotFoundException exp) {
            throw exp;
        } catch (TException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int updateBlobReplication(String key, int replication) throws AuthorizationException, KeyNotFoundException {
        try {
            return client.getClient().updateBlobReplication(key, replication);
        } catch (AuthorizationException | KeyNotFoundException exp) {
            throw exp;
        } catch (TException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean setClient(Map conf, NimbusClient client) {
        this.client = client;
        if (conf != null) {
            this.bufferSize = Utils.getInt(conf.get(Config.STORM_BLOBSTORE_INPUTSTREAM_BUFFER_SIZE_BYTES), bufferSize);
        }
        return true;
    }

    @Override
    protected void finalize() {
        shutdown();
    }

    @Override
    public void shutdown() {
        if (client != null) {
            client.close();
            client = null;
        }
    }
}
