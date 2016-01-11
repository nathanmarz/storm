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
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Very basic blob store impl with no ACL handling.
 */
public class FileBlobStoreImpl {
    private static final long FULL_CLEANUP_FREQ = 60 * 60 * 1000l;
    private static final int BUCKETS = 1024;
    private static final Logger LOG = LoggerFactory.getLogger(FileBlobStoreImpl.class);
    private static final Timer timer = new Timer("FileBlobStore cleanup thread", true);

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
                File dir = new File(fullPath, name);
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

    private File fullPath;
    private TimerTask cleanup = null;

    public FileBlobStoreImpl(File path, Map<String, Object> conf) throws IOException {
        LOG.info("Creating new blob store based in {}", path);
        fullPath = path;
        fullPath.mkdirs();
        Object shouldCleanup = conf.get(Config.BLOBSTORE_CLEANUP_ENABLE);
        if (Utils.getBoolean(shouldCleanup, false)) {
            LOG.debug("Starting File blobstore cleaner");
            cleanup = new TimerTask() {
                @Override
                public void run() {
                    try {
                        fullCleanup(FULL_CLEANUP_FREQ);
                    } catch (IOException e) {
                        LOG.error("Error trying to cleanup", e);
                    }
                }
            };
            timer.scheduleAtFixedRate(cleanup, 0, FULL_CLEANUP_FREQ);
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
     * @param key the key of the part to read.
     * @return the where to read the data from.
     * @throws IOException on any error
     */
    public LocalFsBlobStoreFile read(String key) throws IOException {
        return new LocalFsBlobStoreFile(getKeyDir(key), BlobStoreFile.BLOBSTORE_DATA_FILE);
    }

    /**
     * Get an object tied to writing the data.
     * @param key the key of the part to write to.
     * @return an object that can be used to both write to, but also commit/cancel the operation.
     * @throws IOException on any error
     */
    public LocalFsBlobStoreFile write(String key, boolean create) throws IOException {
        return new LocalFsBlobStoreFile(getKeyDir(key), true, create);
    }

    /**
     * Check if the key exists in the blob store.
     * @param key the key to check for
     * @return true if it exists else false.
     */
    public boolean exists(String key) {
        return getKeyDir(key).exists();
    }

    /**
     * Delete a key from the blob store
     * @param key the key to delete
     * @throws IOException on any error
     */
    public void deleteKey(String key) throws IOException {
        File keyDir = getKeyDir(key);
        LocalFsBlobStoreFile pf = new LocalFsBlobStoreFile(keyDir, BlobStoreFile.BLOBSTORE_DATA_FILE);
        pf.delete();
        delete(keyDir);
    }

    private File getKeyDir(String key) {
        String hash = String.valueOf(Math.abs((long)key.hashCode()) % BUCKETS);
        File ret = new File(new File(fullPath, hash), key);
        LOG.debug("{} Looking for {} in {}", new Object[]{fullPath, key, hash});
        return ret;
    }

    public void fullCleanup(long age) throws IOException {
        long cleanUpIfBefore = System.currentTimeMillis() - age;
        Iterator<String> keys = new KeyInHashDirIterator();
        while (keys.hasNext()) {
            String key = keys.next();
            File keyDir = getKeyDir(key);
            Iterator<LocalFsBlobStoreFile> i = listBlobStoreFiles(keyDir);
            if (!i.hasNext()) {
                //The dir is empty, so try to delete it, may fail, but that is OK
                try {
                    keyDir.delete();
                } catch (Exception e) {
                    LOG.warn("Could not delete "+keyDir+" will try again later");
                }
            }
            while (i.hasNext()) {
                LocalFsBlobStoreFile f = i.next();
                if (f.isTmp()) {
                    if (f.getModTime() <= cleanUpIfBefore) {
                        f.delete();
                    }
                }
            }
        }
    }

    protected Iterator<LocalFsBlobStoreFile> listBlobStoreFiles(File path) throws IOException {
        ArrayList<LocalFsBlobStoreFile> ret = new ArrayList<LocalFsBlobStoreFile>();
        File[] files = path.listFiles();
        if (files != null) {
            for (File sub: files) {
                try {
                    ret.add(new LocalFsBlobStoreFile(sub.getParentFile(), sub.getName()));
                } catch (IllegalArgumentException e) {
                    //Ignored the file did not match
                    LOG.warn("Found an unexpected file in {} {}",path, sub.getName());
                }
            }
        }
        return ret.iterator();
    }

    protected Iterator<String> listKeys(File path) throws IOException {
        String[] files = path.list();
        if (files != null) {
            return Arrays.asList(files).iterator();
        }
        return new LinkedList<String>().iterator();
    }

    protected void delete(File path) throws IOException {
        Files.deleteIfExists(path.toPath());
    }

    public void shutdown() {
        if (cleanup != null) {
            cleanup.cancel();
            cleanup = null;
        }
    }
}
