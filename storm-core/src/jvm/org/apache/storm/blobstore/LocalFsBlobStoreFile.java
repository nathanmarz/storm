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

import org.apache.storm.generated.SettableBlobMeta;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.regex.Matcher;

public class LocalFsBlobStoreFile extends BlobStoreFile {

    private final String _key;
    private final boolean _isTmp;
    private final File _path;
    private Long _modTime = null;
    private final boolean _mustBeNew;
    private SettableBlobMeta meta;

    public LocalFsBlobStoreFile(File base, String name) {
        if (BlobStoreFile.BLOBSTORE_DATA_FILE.equals(name)) {
            _isTmp = false;
        } else {
            Matcher m = TMP_NAME_PATTERN.matcher(name);
            if (!m.matches()) {
                throw new IllegalArgumentException("File name does not match '"+name+"' !~ "+TMP_NAME_PATTERN);
            }
            _isTmp = true;
        }
        _key = base.getName();
        _path = new File(base, name);
        _mustBeNew = false;
    }

    public LocalFsBlobStoreFile(File base, boolean isTmp, boolean mustBeNew) {
        _key = base.getName();
        _isTmp = isTmp;
        _mustBeNew = mustBeNew;
        if (_isTmp) {
            _path = new File(base, System.currentTimeMillis()+TMP_EXT);
        } else {
            _path = new File(base, BlobStoreFile.BLOBSTORE_DATA_FILE);
        }
    }

    @Override
    public void delete() throws IOException {
        _path.delete();
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
            _modTime = _path.lastModified();
        }
        return _modTime;
    }

    @Override
    public InputStream getInputStream() throws IOException {
        if (isTmp()) {
            throw new IllegalStateException("Cannot read from a temporary part file.");
        }
        return new FileInputStream(_path);
    }

    @Override
    public OutputStream getOutputStream() throws IOException {
        if (!isTmp()) {
            throw new IllegalStateException("Can only write to a temporary part file.");
        }
        boolean success = false;
        try {
            success = _path.createNewFile();
        } catch (IOException e) {
            //Try to create the parent directory, may not work
            _path.getParentFile().mkdirs();
            success = _path.createNewFile();
        }
        if (!success) {
            throw new IOException(_path+" already exists");
        }
        return new FileOutputStream(_path);
    }

    @Override
    public void commit() throws IOException {
        if (!isTmp()) {
            throw new IllegalStateException("Can only write to a temporary part file.");
        }

        File dest = new File(_path.getParentFile(), BlobStoreFile.BLOBSTORE_DATA_FILE);
        if (_mustBeNew) {
            Files.move(_path.toPath(), dest.toPath(), StandardCopyOption.ATOMIC_MOVE);
        } else {
            Files.move(_path.toPath(), dest.toPath(), StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    @Override
    public void cancel() throws IOException {
        if (!isTmp()) {
            throw new IllegalStateException("Can only write to a temporary part file.");
        }
        delete();
    }

    @Override
    public SettableBlobMeta getMetadata () {
        return meta;
    }

    @Override
    public void setMetadata (SettableBlobMeta meta) {
        this.meta = meta;
    }

    @Override
    public String toString() {
        return _path+":"+(_isTmp ? "tmp": BlobStoreFile.BLOBSTORE_DATA_FILE)+":"+_key;
    }

    @Override
    public long getFileLength() {
        return _path.length();
    }
}

