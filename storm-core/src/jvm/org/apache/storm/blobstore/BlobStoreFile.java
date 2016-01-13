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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.regex.Pattern;

/**
 * Provides an base implementation for creating a blobstore based on file backed storage.
 */
public abstract class BlobStoreFile {
    public static final Logger LOG = LoggerFactory.getLogger(BlobStoreFile.class);

    protected static final String TMP_EXT = ".tmp";
    protected static final Pattern TMP_NAME_PATTERN = Pattern.compile("^\\d+\\" + TMP_EXT + "$");
    protected static final String BLOBSTORE_DATA_FILE = "data";

    public abstract void delete() throws IOException;
    public abstract String getKey();
    public abstract boolean isTmp();
    public abstract void setMetadata(SettableBlobMeta meta);
    public abstract SettableBlobMeta getMetadata();
    public abstract long getModTime() throws IOException;
    public abstract InputStream getInputStream() throws IOException;
    public abstract OutputStream getOutputStream() throws IOException;
    public abstract void commit() throws IOException;
    public abstract void cancel() throws IOException;
    public abstract long getFileLength() throws IOException;
}
