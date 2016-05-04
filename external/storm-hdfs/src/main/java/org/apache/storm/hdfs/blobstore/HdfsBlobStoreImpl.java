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
package org.apache.storm.hdfs.blobstore;

import org.apache.storm.Config;
import org.apache.storm.blobstore.BlobStoreFile;
import org.apache.storm.utils.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Timer;
import java.util.TimerTask;

/**
 * HDFS blob store impl.
 */
public class HdfsBlobStoreImpl {
    private static final Logger LOG = LoggerFactory.getLogger(HdfsBlobStoreImpl.class);

    private static final long FULL_CLEANUP_FREQ = 60 * 60 * 1000l;
    private static final int BUCKETS = 1024;
    private static final Timer timer = new Timer("HdfsBlobStore cleanup thread", true);
    private static final String BLOBSTORE_DATA = "data";

    public class KeyInHashDirIterator implements Iterator<String> {
        private int currentBucket = 0;
        private Iterator<String> it = null;
        private String next = null;

        public KeyInHashDirIterator() throws IOException {
            primeNext();
        }

        private void primeNext() throws IOException {
            while (it == null && currentBucket < BUCKETS) {
                String name = String.valueOf(currentBucket);
                Path dir = new Path(_fullPath, name);
                try {
                    it = listKeys(dir);
                } catch (FileNotFoundException e) {
                    it = null;
                }
                if (it == null || !it.hasNext()) {
                    it = null;
                    currentBucket++;
                } else {
                    next = it.next();
                }
            }
        }

        @Override
        public boolean hasNext() {
            return next != null;
        }

        @Override
        public String next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            String current = next;
            next = null;
            if (it != null) {
                if (!it.hasNext()) {
                    it = null;
                    currentBucket++;
                    try {
                        primeNext();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                } else {
                    next = it.next();
                }
            }
            return current;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Delete Not Supported");
        }
    }


    private Path _fullPath;
    private FileSystem _fs;
    private TimerTask _cleanup = null;
    private Configuration _hadoopConf;

    // blobstore directory is private!
    final public static FsPermission BLOBSTORE_DIR_PERMISSION =
            FsPermission.createImmutable((short) 0700); // rwx--------

    public HdfsBlobStoreImpl(Path path, Map<String, Object> conf) throws IOException {
        this(path, conf, new Configuration());
    }

    public HdfsBlobStoreImpl(Path path, Map<String, Object> conf,
                             Configuration hconf) throws IOException {
        LOG.info("Blob store based in {}", path);
        _fullPath = path;
        _hadoopConf = hconf;
        _fs = path.getFileSystem(_hadoopConf);

        if (!_fs.exists(_fullPath)) {
            FsPermission perms = new FsPermission(BLOBSTORE_DIR_PERMISSION);
            boolean success = _fs.mkdirs(_fullPath, perms);
            if (!success) {
                throw new IOException("Error creating blobstore directory: " + _fullPath);
            }
        }

        Object shouldCleanup = conf.get(Config.BLOBSTORE_CLEANUP_ENABLE);
        if (Utils.getBoolean(shouldCleanup, false)) {
            LOG.debug("Starting hdfs blobstore cleaner");
            _cleanup = new TimerTask() {
                @Override
                public void run() {
                    try {
                        fullCleanup(FULL_CLEANUP_FREQ);
                    } catch (IOException e) {
                        LOG.error("Error trying to cleanup", e);
                    }
                }
            };
            timer.scheduleAtFixedRate(_cleanup, 0, FULL_CLEANUP_FREQ);
        }
    }

    /**
     * @return all keys that are available for reading.
     * @throws IOException on any error.
     */
    public Iterator<String> listKeys() throws IOException {
        return new KeyInHashDirIterator();
    }

    /**
     * Get an input stream for reading a part.
     *
     * @param key the key of the part to read.
     * @return the where to read the data from.
     * @throws IOException on any error
     */
    public BlobStoreFile read(String key) throws IOException {
        return new HdfsBlobStoreFile(getKeyDir(key), BLOBSTORE_DATA, _hadoopConf);
    }

    /**
     * Get an object tied to writing the data.
     *
     * @param key the key of the part to write to.
     * @param create whether the file needs to be new or not.
     * @return an object that can be used to both write to, but also commit/cancel the operation.
     * @throws IOException on any error
     */
    public BlobStoreFile write(String key, boolean create) throws IOException {
        return new HdfsBlobStoreFile(getKeyDir(key), true, create, _hadoopConf);
    }

    /**
     * Check if the key exists in the blob store.
     *
     * @param key the key to check for
     * @return true if it exists else false.
     */
    public boolean exists(String key) {
        Path dir = getKeyDir(key);
        boolean res = false;
        try {
            _fs = dir.getFileSystem(_hadoopConf);
            res = _fs.exists(dir);
        } catch (IOException e) {
            LOG.warn("Exception checking for exists on: " + key);
        }
        return res;
    }

    /**
     * Delete a key from the blob store
     *
     * @param key the key to delete
     * @throws IOException on any error
     */
    public void deleteKey(String key) throws IOException {
        Path keyDir = getKeyDir(key);
        HdfsBlobStoreFile pf = new HdfsBlobStoreFile(keyDir, BLOBSTORE_DATA,
                _hadoopConf);
        pf.delete();
        delete(keyDir);
    }

    protected Path getKeyDir(String key) {
        String hash = String.valueOf(Math.abs((long) key.hashCode()) % BUCKETS);
        Path hashDir = new Path(_fullPath, hash);

        Path ret = new Path(hashDir, key);
        LOG.debug("{} Looking for {} in {}", new Object[]{_fullPath, key, hash});
        return ret;
    }

    public void fullCleanup(long age) throws IOException {
        long cleanUpIfBefore = System.currentTimeMillis() - age;
        Iterator<String> keys = new KeyInHashDirIterator();
        while (keys.hasNext()) {
            String key = keys.next();
            Path keyDir = getKeyDir(key);
            Iterator<BlobStoreFile> i = listBlobStoreFiles(keyDir);
            if (!i.hasNext()) {
                //The dir is empty, so try to delete it, may fail, but that is OK
                try {
                    _fs.delete(keyDir, true);
                } catch (Exception e) {
                    LOG.warn("Could not delete " + keyDir + " will try again later");
                }
            }
            while (i.hasNext()) {
                BlobStoreFile f = i.next();
                if (f.isTmp()) {
                    if (f.getModTime() <= cleanUpIfBefore) {
                        f.delete();
                    }
                }
            }
        }
    }

    protected Iterator<BlobStoreFile> listBlobStoreFiles(Path path) throws IOException {
        ArrayList<BlobStoreFile> ret = new ArrayList<BlobStoreFile>();
        FileStatus[] files = _fs.listStatus(new Path[]{path});
        if (files != null) {
            for (FileStatus sub : files) {
                try {
                    ret.add(new HdfsBlobStoreFile(sub.getPath().getParent(), sub.getPath().getName(),
                            _hadoopConf));
                } catch (IllegalArgumentException e) {
                    //Ignored the file did not match
                    LOG.warn("Found an unexpected file in {} {}", path, sub.getPath().getName());
                }
            }
        }
        return ret.iterator();
    }

    protected Iterator<String> listKeys(Path path) throws IOException {
        ArrayList<String> ret = new ArrayList<String>();
        FileStatus[] files = _fs.listStatus(new Path[]{path});
        if (files != null) {
            for (FileStatus sub : files) {
                try {
                    ret.add(sub.getPath().getName().toString());
                } catch (IllegalArgumentException e) {
                    //Ignored the file did not match
                    LOG.debug("Found an unexpected file in {} {}", path, sub.getPath().getName());
                }
            }
        }
        return ret.iterator();
    }

    protected int getBlobReplication(String key) throws IOException {
        Path path = getKeyDir(key);
        Path dest = new Path(path, BLOBSTORE_DATA);
        return _fs.getFileStatus(dest).getReplication();
    }

    protected int updateBlobReplication(String key, int replication) throws IOException {
        Path path = getKeyDir(key);
        Path dest = new Path(path, BLOBSTORE_DATA);
        _fs.setReplication(dest, (short) replication);
        return _fs.getFileStatus(dest).getReplication();
    }

    protected void delete(Path path) throws IOException {
        _fs.delete(path, true);
    }

    public void shutdown() {
        if (_cleanup != null) {
            _cleanup.cancel();
            _cleanup = null;
        }
    }
}
