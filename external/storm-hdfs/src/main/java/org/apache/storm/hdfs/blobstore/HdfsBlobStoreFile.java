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

import org.apache.storm.blobstore.BlobStoreFile;
import org.apache.storm.generated.SettableBlobMeta;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.regex.Matcher;

public class HdfsBlobStoreFile extends BlobStoreFile {
    public static final Logger LOG = LoggerFactory.getLogger(HdfsBlobStoreFile.class);

    private final String _key;
    private final boolean _isTmp;
    private final Path _path;
    private Long _modTime = null;
    private final boolean _mustBeNew;
    private final Configuration _hadoopConf;
    private final FileSystem _fs;
    private SettableBlobMeta meta;

    // files are world-wide readable and owner writable
    final public static FsPermission BLOBSTORE_FILE_PERMISSION =
            FsPermission.createImmutable((short) 0644); // rw-r--r--

    public HdfsBlobStoreFile(Path base, String name, Configuration hconf) {
        if (BLOBSTORE_DATA_FILE.equals(name)) {
            _isTmp = false;
        } else {
            Matcher m = TMP_NAME_PATTERN.matcher(name);
            if (!m.matches()) {
                throw new IllegalArgumentException("File name does not match '"+name+"' !~ "+TMP_NAME_PATTERN);
            }
            _isTmp = true;
        }
        _hadoopConf = hconf;
        _key = base.getName();
        _path = new Path(base, name);
        _mustBeNew = false;
        try {
            _fs = _path.getFileSystem(_hadoopConf);
        } catch (IOException e) {
            throw new RuntimeException("Error getting filesystem for path: " + _path, e);
        }
    }

    public HdfsBlobStoreFile(Path base, boolean isTmp, boolean mustBeNew, Configuration hconf) {
        _key = base.getName();
        _hadoopConf = hconf;
        _isTmp = isTmp;
        _mustBeNew = mustBeNew;
        if (_isTmp) {
            _path = new Path(base, System.currentTimeMillis()+TMP_EXT);
        } else {
            _path = new Path(base, BLOBSTORE_DATA_FILE);
        }
        try {
            _fs = _path.getFileSystem(_hadoopConf);
        } catch (IOException e) {
            throw new RuntimeException("Error getting filesystem for path: " + _path, e);
        }
    }

    @Override
    public void delete() throws IOException {
        _fs.delete(_path, true);
    }

    @Override
    public boolean isTmp() {
        return _isTmp;
    }

    @Override
    public String getKey() {
        return _key;
    }

    @Override
    public long getModTime() throws IOException {
        if (_modTime == null) {
            FileSystem fs = _path.getFileSystem(_hadoopConf);
            _modTime = fs.getFileStatus(_path).getModificationTime();
        }
        return _modTime;
    }

    private void checkIsNotTmp() {
        if (!isTmp()) {
            throw new IllegalStateException("Can only operate on a temporary blobstore file.");
        }
    }

    private void checkIsTmp() {
        if (isTmp()) {
            throw new IllegalStateException("Cannot operate on a temporary blobstore file.");
        }
    }

    @Override
    public InputStream getInputStream() throws IOException {
        checkIsTmp();
        return _fs.open(_path);
    }

    @Override
    public OutputStream getOutputStream() throws IOException {
        checkIsNotTmp();
        OutputStream out = null;
        FsPermission fileperms = new FsPermission(BLOBSTORE_FILE_PERMISSION);
        try {
            out = _fs.create(_path, (short)this.getMetadata().get_replication_factor());
            _fs.setPermission(_path, fileperms);
            _fs.setReplication(_path, (short)this.getMetadata().get_replication_factor());
        } catch (IOException e) {
            //Try to create the parent directory, may not work
            FsPermission dirperms = new FsPermission(HdfsBlobStoreImpl.BLOBSTORE_DIR_PERMISSION);
            if (!_fs.mkdirs(_path.getParent(), dirperms)) {
                LOG.warn("error creating parent dir: " + _path.getParent());
            }
            out = _fs.create(_path, (short)this.getMetadata().get_replication_factor());
            _fs.setPermission(_path, dirperms);
            _fs.setReplication(_path, (short)this.getMetadata().get_replication_factor());
        }
        if (out == null) {
            throw new IOException("Error in creating: " + _path);
        }
        return out;
    }

    @Override
    public void commit() throws IOException {
        checkIsNotTmp();
        // FileContext supports atomic rename, whereas FileSystem doesn't
        FileContext fc = FileContext.getFileContext(_hadoopConf);
        Path dest = new Path(_path.getParent(), BLOBSTORE_DATA_FILE);
        if (_mustBeNew) {
            fc.rename(_path, dest);
        } else {
            fc.rename(_path, dest, Options.Rename.OVERWRITE);
        }
        // Note, we could add support for setting the replication factor
    }

    @Override
    public void cancel() throws IOException {
        checkIsNotTmp();
        delete();
    }

    @Override
    public String toString() {
        return _path+":"+(_isTmp ? "tmp": BlobStoreFile.BLOBSTORE_DATA_FILE)+":"+_key;
    }

    @Override
    public long getFileLength() throws IOException {
        return _fs.getFileStatus(_path).getLen();
    }

    @Override
    public SettableBlobMeta getMetadata() {
        return meta;
    }

    @Override
    public void setMetadata(SettableBlobMeta meta) {
        this.meta = meta;
    }
}
