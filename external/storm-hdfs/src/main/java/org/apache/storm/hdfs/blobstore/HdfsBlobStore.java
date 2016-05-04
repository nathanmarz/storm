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
import org.apache.storm.blobstore.AtomicOutputStream;
import org.apache.storm.blobstore.BlobStore;
import org.apache.storm.blobstore.BlobStoreAclHandler;
import org.apache.storm.blobstore.BlobStoreFile;
import org.apache.storm.blobstore.InputStreamWithMeta;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.KeyNotFoundException;
import org.apache.storm.generated.KeyAlreadyExistsException;
import org.apache.storm.generated.ReadableBlobMeta;
import org.apache.storm.generated.SettableBlobMeta;
import org.apache.storm.nimbus.NimbusInfo;
import org.apache.storm.utils.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Iterator;
import java.util.Map;

import static org.apache.storm.blobstore.BlobStoreAclHandler.ADMIN;
import static org.apache.storm.blobstore.BlobStoreAclHandler.READ;
import static org.apache.storm.blobstore.BlobStoreAclHandler.WRITE;

/**
 * Provides a HDFS file system backed blob store implementation.
 * Note that this provides an api for having HDFS be the backing store for the blobstore,
 * it is not a service/daemon.
 *
 * We currently have NIMBUS_ADMINS and SUPERVISOR_ADMINS configuration. NIMBUS_ADMINS are given READ, WRITE and ADMIN
 * access whereas the SUPERVISOR_ADMINS are given READ access in order to read and download the blobs form the nimbus.
 *
 * The ACLs for the blob store are validated against whether the subject is a NIMBUS_ADMIN, SUPERVISOR_ADMIN or USER
 * who has read, write or admin privileges in order to perform respective operations on the blob.
 *
 * For hdfs blob store
 * 1. The USER interacts with nimbus to upload and access blobs through NimbusBlobStore Client API. Here, unlike
 * local blob store which stores the blobs locally, the nimbus talks to HDFS to upload the blobs.
 * 2. The USER sets the ACLs, and the blob access is validated against these ACLs.
 * 3. The SUPERVISOR interacts with nimbus through HdfsClientBlobStore to download the blobs. Here, unlike local
 * blob store the supervisor interacts with HDFS directly to download the blobs. The call to HdfsBlobStore is made as a "null"
 * subject. The blobstore gets the hadoop user and validates permissions for the supervisor.
 */
public class HdfsBlobStore extends BlobStore {
    public static final Logger LOG = LoggerFactory.getLogger(HdfsBlobStore.class);
    private static final String DATA_PREFIX = "data_";
    private static final String META_PREFIX = "meta_";
    private BlobStoreAclHandler _aclHandler;
    private HdfsBlobStoreImpl _hbs;
    private Subject _localSubject;
    private Map conf;

    /**
     * Get the subject from Hadoop so we can use it to validate the acls. There is no direct
     * interface from UserGroupInformation to get the subject, so do a doAs and get the context.
     * We could probably run everything in the doAs but for now just grab the subject.
     */
    private Subject getHadoopUser() {
        Subject subj;
        try {
            subj = UserGroupInformation.getCurrentUser().doAs(
                    new PrivilegedAction<Subject>() {
                        @Override
                        public Subject run() {
                            return Subject.getSubject(AccessController.getContext());
                        }
                    });
        } catch (IOException e) {
            throw new RuntimeException("Error creating subject and logging user in!", e);
        }
        return subj;
    }

    /**
     * If who is null then we want to use the user hadoop says we are.
     * Required for the supervisor to call these routines as its not
     * logged in as anyone.
     */
    private Subject checkAndGetSubject(Subject who) {
        if (who == null) {
            return _localSubject;
        }
        return who;
    }

    @Override
    public void prepare(Map conf, String overrideBase, NimbusInfo nimbusInfo) {
        this.conf = conf;
        prepareInternal(conf, overrideBase, null);
    }

    /**
     * Allow a Hadoop Configuration to be passed for testing. If it's null then the hadoop configs
     * must be in your classpath.
     */
    protected void prepareInternal(Map conf, String overrideBase, Configuration hadoopConf) {
        this.conf = conf;
        if (overrideBase == null) {
            overrideBase = (String)conf.get(Config.BLOBSTORE_DIR);
        }
        if (overrideBase == null) {
            throw new RuntimeException("You must specify a blobstore directory for HDFS to use!");
        }
        LOG.debug("directory is: {}", overrideBase);
        try {
            // if a HDFS keytab/principal have been supplied login, otherwise assume they are
            // logged in already or running insecure HDFS.
            String principal = (String) conf.get(Config.BLOBSTORE_HDFS_PRINCIPAL);
            String keyTab = (String) conf.get(Config.BLOBSTORE_HDFS_KEYTAB);

            if (principal != null && keyTab != null) {
                UserGroupInformation.loginUserFromKeytab(principal, keyTab);
            } else {
                if (principal == null && keyTab != null) {
                    throw new RuntimeException("You must specify an HDFS principal to go with the keytab!");

                } else {
                    if (principal != null && keyTab == null) {
                        throw new RuntimeException("You must specify HDFS keytab go with the principal!");
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Error logging in from keytab!", e);
        }
        Path baseDir = new Path(overrideBase, BASE_BLOBS_DIR_NAME);
        try {
            if (hadoopConf != null) {
                _hbs = new HdfsBlobStoreImpl(baseDir, conf, hadoopConf);
            } else {
                _hbs = new HdfsBlobStoreImpl(baseDir, conf);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        _localSubject = getHadoopUser();
        _aclHandler = new BlobStoreAclHandler(conf);
    }

    @Override
    public AtomicOutputStream createBlob(String key, SettableBlobMeta meta, Subject who)
            throws AuthorizationException, KeyAlreadyExistsException {
        if (meta.get_replication_factor() <= 0) {
            meta.set_replication_factor((int)conf.get(Config.STORM_BLOBSTORE_REPLICATION_FACTOR));
        }
        who = checkAndGetSubject(who);
        validateKey(key);
        _aclHandler.normalizeSettableBlobMeta(key, meta, who, READ | WRITE | ADMIN);
        BlobStoreAclHandler.validateSettableACLs(key, meta.get_acl());
        _aclHandler.hasPermissions(meta.get_acl(), READ | WRITE | ADMIN, who, key);
        if (_hbs.exists(DATA_PREFIX+key)) {
            throw new KeyAlreadyExistsException(key);
        }
        BlobStoreFileOutputStream mOut = null;
        try {
            BlobStoreFile metaFile = _hbs.write(META_PREFIX + key, true);
            metaFile.setMetadata(meta);
            mOut = new BlobStoreFileOutputStream(metaFile);
            mOut.write(Utils.thriftSerialize(meta));
            mOut.close();
            mOut = null;
            BlobStoreFile dataFile = _hbs.write(DATA_PREFIX + key, true);
            dataFile.setMetadata(meta);
            return new BlobStoreFileOutputStream(dataFile);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            if (mOut != null) {
                try {
                    mOut.cancel();
                } catch (IOException e) {
                    //Ignored
                }
            }
        }
    }

    @Override
    public AtomicOutputStream updateBlob(String key, Subject who)
            throws AuthorizationException, KeyNotFoundException {
        who = checkAndGetSubject(who);
        SettableBlobMeta meta = getStoredBlobMeta(key);
        validateKey(key);
        _aclHandler.hasPermissions(meta.get_acl(), WRITE, who, key);
        try {
            BlobStoreFile dataFile = _hbs.write(DATA_PREFIX + key, false);
            dataFile.setMetadata(meta);
            return new BlobStoreFileOutputStream(dataFile);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private SettableBlobMeta getStoredBlobMeta(String key) throws KeyNotFoundException {
        InputStream in = null;
        try {
            BlobStoreFile pf = _hbs.read(META_PREFIX + key);
            try {
                in = pf.getInputStream();
            } catch (FileNotFoundException fnf) {
                throw new KeyNotFoundException(key);
            }
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            byte[] buffer = new byte[2048];
            int len;
            while ((len = in.read(buffer)) > 0) {
                out.write(buffer, 0, len);
            }
            in.close();
            in = null;
            return Utils.thriftDeserialize(SettableBlobMeta.class, out.toByteArray());
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    //Ignored
                }
            }
        }
    }

    @Override
    public ReadableBlobMeta getBlobMeta(String key, Subject who)
            throws AuthorizationException, KeyNotFoundException {
        who = checkAndGetSubject(who);
        validateKey(key);
        SettableBlobMeta meta = getStoredBlobMeta(key);
        _aclHandler.validateUserCanReadMeta(meta.get_acl(), who, key);
        ReadableBlobMeta rbm = new ReadableBlobMeta();
        rbm.set_settable(meta);
        try {
            BlobStoreFile pf = _hbs.read(DATA_PREFIX + key);
            rbm.set_version(pf.getModTime());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return rbm;
    }

    @Override
    public void setBlobMeta(String key, SettableBlobMeta meta, Subject who)
            throws AuthorizationException, KeyNotFoundException {
        if (meta.get_replication_factor() <= 0) {
            meta.set_replication_factor((int)conf.get(Config.STORM_BLOBSTORE_REPLICATION_FACTOR));
        }
        who = checkAndGetSubject(who);
        validateKey(key);
        _aclHandler.normalizeSettableBlobMeta(key,  meta, who, ADMIN);
        BlobStoreAclHandler.validateSettableACLs(key, meta.get_acl());
        SettableBlobMeta orig = getStoredBlobMeta(key);
        _aclHandler.hasPermissions(orig.get_acl(), ADMIN, who, key);
        BlobStoreFileOutputStream mOut = null;
        writeMetadata(key, meta);
    }

    @Override
    public void deleteBlob(String key, Subject who)
            throws AuthorizationException, KeyNotFoundException {
        who = checkAndGetSubject(who);
        validateKey(key);
        SettableBlobMeta meta = getStoredBlobMeta(key);
        _aclHandler.hasPermissions(meta.get_acl(), WRITE, who, key);
        try {
            _hbs.deleteKey(DATA_PREFIX + key);
            _hbs.deleteKey(META_PREFIX + key);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public InputStreamWithMeta getBlob(String key, Subject who)
            throws AuthorizationException, KeyNotFoundException {
        who = checkAndGetSubject(who);
        validateKey(key);
        SettableBlobMeta meta = getStoredBlobMeta(key);
        _aclHandler.hasPermissions(meta.get_acl(), READ, who, key);
        try {
            return new BlobStoreFileInputStream(_hbs.read(DATA_PREFIX + key));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Iterator<String> listKeys() {
        try {
            return new KeyTranslationIterator(_hbs.listKeys(), DATA_PREFIX);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void shutdown() {
        //Empty
    }

    @Override
    public int getBlobReplication(String key, Subject who) throws AuthorizationException, KeyNotFoundException {
        who = checkAndGetSubject(who);
        validateKey(key);
        SettableBlobMeta meta = getStoredBlobMeta(key);
        _aclHandler.hasAnyPermissions(meta.get_acl(), READ | WRITE | ADMIN, who, key);
        try {
            return _hbs.getBlobReplication(DATA_PREFIX + key);
        } catch (IOException exp) {
            throw new RuntimeException(exp);
        }
    }

    @Override
    public int updateBlobReplication(String key, int replication, Subject who) throws AuthorizationException, KeyNotFoundException {
        who = checkAndGetSubject(who);
        validateKey(key);
        SettableBlobMeta meta = getStoredBlobMeta(key);
        meta.set_replication_factor(replication);
        _aclHandler.hasAnyPermissions(meta.get_acl(), WRITE | ADMIN, who, key);
        try {
            writeMetadata(key, meta);
            return _hbs.updateBlobReplication(DATA_PREFIX + key, replication);
        } catch (IOException exp) {
            throw new RuntimeException(exp);
        }
    }

    public void writeMetadata(String key, SettableBlobMeta meta)
            throws AuthorizationException, KeyNotFoundException {
        BlobStoreFileOutputStream mOut = null;
        try {
            BlobStoreFile hdfsFile = _hbs.write(META_PREFIX + key, false);
            hdfsFile.setMetadata(meta);
            mOut = new BlobStoreFileOutputStream(hdfsFile);
            mOut.write(Utils.thriftSerialize(meta));
            mOut.close();
            mOut = null;
        } catch (IOException exp) {
            throw new RuntimeException(exp);
        } finally {
            if (mOut != null) {
                try {
                    mOut.cancel();
                } catch (IOException e) {
                    //Ignored
                }
            }
        }
    }

    public void fullCleanup(long age) throws IOException {
        _hbs.fullCleanup(age);
    }
}
