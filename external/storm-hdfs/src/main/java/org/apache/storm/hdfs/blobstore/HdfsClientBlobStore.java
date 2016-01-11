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

import org.apache.storm.blobstore.AtomicOutputStream;
import org.apache.storm.blobstore.ClientBlobStore;
import org.apache.storm.blobstore.InputStreamWithMeta;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.ReadableBlobMeta;
import org.apache.storm.generated.SettableBlobMeta;
import org.apache.storm.generated.KeyAlreadyExistsException;
import org.apache.storm.generated.KeyNotFoundException;
import org.apache.storm.utils.NimbusClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;

/**
 *  Client to access the HDFS blobStore. At this point, this is meant to only be used by the
 *  supervisor.  Don't trust who the client says they are so pass null for all Subjects.
 *
 *  The HdfsBlobStore implementation takes care of the null Subjects. It assigns Subjects
 *  based on what hadoop says who the users are. These users must be configured accordingly
 *  in the SUPERVISOR_ADMINS for ACL validation and for the supervisors to download the blobs.
 *  This API is only used by the supervisor in order to talk directly to HDFS.
 */
public class HdfsClientBlobStore extends ClientBlobStore {
    private static final Logger LOG = LoggerFactory.getLogger(HdfsClientBlobStore.class);
    private HdfsBlobStore _blobStore;
    private Map _conf;

    @Override
    public void prepare(Map conf) {
        this._conf = conf;
        _blobStore = new HdfsBlobStore();
        _blobStore.prepare(conf, null, null);
    }

    @Override
    public AtomicOutputStream createBlobToExtend(String key, SettableBlobMeta meta)
            throws AuthorizationException, KeyAlreadyExistsException {
        return _blobStore.createBlob(key, meta, null);
    }

    @Override
    public AtomicOutputStream updateBlob(String key)
            throws AuthorizationException, KeyNotFoundException {
        return _blobStore.updateBlob(key, null);
    }

    @Override
    public ReadableBlobMeta getBlobMeta(String key)
            throws AuthorizationException, KeyNotFoundException {
        return _blobStore.getBlobMeta(key, null);
    }

    @Override
    public void setBlobMetaToExtend(String key, SettableBlobMeta meta)
            throws AuthorizationException, KeyNotFoundException {
        _blobStore.setBlobMeta(key, meta, null);
    }

    @Override
    public void deleteBlob(String key) throws AuthorizationException, KeyNotFoundException {
        _blobStore.deleteBlob(key, null);
    }

    @Override
    public InputStreamWithMeta getBlob(String key)
            throws AuthorizationException, KeyNotFoundException {
        return _blobStore.getBlob(key, null);
    }

    @Override
    public Iterator<String> listKeys() {
        return _blobStore.listKeys();
    }

    @Override
    public int getBlobReplication(String key) throws AuthorizationException, KeyNotFoundException {
        return _blobStore.getBlobReplication(key, null);
    }

    @Override
    public int updateBlobReplication(String key, int replication) throws AuthorizationException, KeyNotFoundException {
        return _blobStore.updateBlobReplication(key, replication, null);
    }

    @Override
    public boolean setClient(Map conf, NimbusClient client) {
        return true;
    }

    @Override
    public void createStateInZookeeper(String key) {
        // Do nothing
    }

    @Override
    public void shutdown() {
        // do nothing
    }
}
