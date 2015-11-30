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
package backtype.storm.blobstore;

import backtype.storm.daemon.Shutdownable;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.ReadableBlobMeta;
import backtype.storm.generated.SettableBlobMeta;
import backtype.storm.generated.KeyAlreadyExistsException;
import backtype.storm.generated.KeyNotFoundException;
import backtype.storm.utils.NimbusClient;

import java.util.Iterator;
import java.util.Map;

public abstract class ClientBlobStore implements Shutdownable {
    protected Map conf;

    public abstract void prepare(Map conf);
    protected abstract AtomicOutputStream createBlobToExtend(String key, SettableBlobMeta meta) throws AuthorizationException, KeyAlreadyExistsException;
    public abstract AtomicOutputStream updateBlob(String key) throws AuthorizationException, KeyNotFoundException;
    public abstract ReadableBlobMeta getBlobMeta(String key) throws AuthorizationException, KeyNotFoundException;
    protected abstract void setBlobMetaToExtend(String key, SettableBlobMeta meta) throws AuthorizationException, KeyNotFoundException;
    public abstract void deleteBlob(String key) throws AuthorizationException, KeyNotFoundException;
    public abstract InputStreamWithMeta getBlob(String key) throws AuthorizationException, KeyNotFoundException;
    public abstract Iterator<String> listKeys();
    public abstract int getBlobReplication(String Key) throws AuthorizationException, KeyNotFoundException;
    public abstract int updateBlobReplication(String Key, int replication) throws AuthorizationException, KeyNotFoundException;
    public abstract boolean setClient(Map conf, NimbusClient client);
    public abstract void createStateInZookeeper(String key);

    public final AtomicOutputStream createBlob(String key, SettableBlobMeta meta) throws AuthorizationException, KeyAlreadyExistsException {
        if (meta !=null && meta.is_set_acl()) {
            BlobStoreAclHandler.validateSettableACLs(key, meta.get_acl());
        }
        return createBlobToExtend(key, meta);
    }

    public final void setBlobMeta(String key, SettableBlobMeta meta) throws AuthorizationException, KeyNotFoundException {
        if (meta !=null && meta.is_set_acl()) {
            BlobStoreAclHandler.validateSettableACLs(key, meta.get_acl());
        }
        setBlobMetaToExtend(key, meta);
    }


}
