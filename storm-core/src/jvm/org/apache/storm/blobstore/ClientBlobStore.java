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

import org.apache.storm.daemon.Shutdownable;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.ReadableBlobMeta;
import org.apache.storm.generated.SettableBlobMeta;
import org.apache.storm.generated.KeyAlreadyExistsException;
import org.apache.storm.generated.KeyNotFoundException;
import org.apache.storm.utils.NimbusClient;

import java.util.Iterator;
import java.util.Map;

/**
 * The ClientBlobStore has two concrete implementations
 * 1. NimbusBlobStore
 * 2. HdfsClientBlobStore
 *
 * Create, update, read and delete are some of the basic operations defined by this interface.
 * Each operation is validated for permissions against an user. We currently have NIMBUS_ADMINS and SUPERVISOR_ADMINS
 * configuration. NIMBUS_ADMINS are given READ, WRITE and ADMIN access whereas the SUPERVISOR_ADMINS are given READ
 * access in order to read and download the blobs form the nimbus.
 *
 * The ACLs for the blob store are validated against whether the subject is a NIMBUS_ADMIN, SUPERVISOR_ADMIN or USER
 * who has read, write or admin privileges in order to perform respective operations on the blob.
 *
 * For more detailed implementation
 * @see org.apache.storm.blobstore.NimbusBlobStore
 * @see org.apache.storm.blobstore.LocalFsBlobStore
 * @see org.apache.storm.hdfs.blobstore.HdfsClientBlobStore
 * @see org.apache.storm.hdfs.blobstore.HdfsBlobStore
 */
public abstract class ClientBlobStore implements Shutdownable {
    protected Map conf;

    /**
     * Sets up the client API by parsing the configs.
     * @param conf The storm conf containing the config details.
     */
    public abstract void prepare(Map conf);

    /**
     * Client facing API to create a blob.
     * @param key blob key name.
     * @param meta contains ACL information.
     * @return AtomicOutputStream returns an output stream into which data can be written.
     * @throws AuthorizationException
     * @throws KeyAlreadyExistsException
     */
    protected abstract AtomicOutputStream createBlobToExtend(String key, SettableBlobMeta meta) throws AuthorizationException, KeyAlreadyExistsException;

    /**
     * Client facing API to update a blob.
     * @param key blob key name.
     * @return AtomicOutputStream returns an output stream into which data can be written.
     * @throws AuthorizationException
     * @throws KeyNotFoundException
     */
    public abstract AtomicOutputStream updateBlob(String key) throws AuthorizationException, KeyNotFoundException;

    /**
     * Client facing API to read the metadata information.
     * @param key blob key name.
     * @return AtomicOutputStream returns an output stream into which data can be written.
     * @throws AuthorizationException
     * @throws KeyNotFoundException
     */
    public abstract ReadableBlobMeta getBlobMeta(String key) throws AuthorizationException, KeyNotFoundException;

    /**
     * Client facing API to set the metadata for a blob.
     * @param key blob key name.
     * @param meta contains ACL information.
     * @throws AuthorizationException
     * @throws KeyNotFoundException
     */
    protected abstract void setBlobMetaToExtend(String key, SettableBlobMeta meta) throws AuthorizationException, KeyNotFoundException;

    /**
     * Client facing API to delete a blob.
     * @param key blob key name.
     * @throws AuthorizationException
     * @throws KeyNotFoundException
     */
    public abstract void deleteBlob(String key) throws AuthorizationException, KeyNotFoundException;

    /**
     * Client facing API to read a blob.
     * @param key blob key name.
     * @return an InputStream to read the metadata for a blob.
     * @throws AuthorizationException
     * @throws KeyNotFoundException
     */
    public abstract InputStreamWithMeta getBlob(String key) throws AuthorizationException, KeyNotFoundException;

    /**
     * @return Iterator for a list of keys currently present in the blob store.
     */
    public abstract Iterator<String> listKeys();

    /**
     * Client facing API to read the replication of a blob.
     * @param key blob key name.
     * @return int indicates the replication factor of a blob.
     * @throws AuthorizationException
     * @throws KeyNotFoundException
     */
    public abstract int getBlobReplication(String key) throws AuthorizationException, KeyNotFoundException;

    /**
     * Client facing API to update the replication of a blob.
     * @param key blob key name.
     * @param replication int indicates the replication factor a blob has to be set.
     * @return int indicates the replication factor of a blob.
     * @throws AuthorizationException
     * @throws KeyNotFoundException
     */
    public abstract int updateBlobReplication(String key, int replication) throws AuthorizationException, KeyNotFoundException;

    /**
     * Client facing API to set a nimbus client.
     * @param conf storm conf
     * @param client NimbusClient
     * @return indicates where the client connection has been setup.
     */
    public abstract boolean setClient(Map conf, NimbusClient client);

    /**
     * Creates state inside a zookeeper.
     * Required for blobstore to write to zookeeper
     * when Nimbus HA is turned on in order to maintain
     * state consistency
     * @param key
     */
    public abstract void createStateInZookeeper(String key);

    /**
     * Client facing API to create a blob.
     * @param key blob key name.
     * @param meta contains ACL information.
     * @return AtomicOutputStream returns an output stream into which data can be written.
     * @throws AuthorizationException
     * @throws KeyAlreadyExistsException
     */
    public final AtomicOutputStream createBlob(String key, SettableBlobMeta meta) throws AuthorizationException, KeyAlreadyExistsException {
        if (meta !=null && meta.is_set_acl()) {
            BlobStoreAclHandler.validateSettableACLs(key, meta.get_acl());
        }
        return createBlobToExtend(key, meta);
    }

    /**
     * Client facing API to set the metadata for a blob.
     * @param key blob key name.
     * @param meta contains ACL information.
     * @throws AuthorizationException
     * @throws KeyNotFoundException
     */
    public final void setBlobMeta(String key, SettableBlobMeta meta) throws AuthorizationException, KeyNotFoundException {
        if (meta !=null && meta.is_set_acl()) {
            BlobStoreAclHandler.validateSettableACLs(key, meta.get_acl());
        }
        setBlobMetaToExtend(key, meta);
    }


}
