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
package org.apache.storm.hdfs.common;

import org.apache.hadoop.fs.Path;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.tuple.Tuple;

import java.io.IOException;

abstract public class AbstractHDFSWriter {
    long lastUsedTime;
    long offset;
    boolean needsRotation;
    Path filePath;
    FileRotationPolicy rotationPolicy;

    AbstractHDFSWriter(FileRotationPolicy policy, Path path) {
        //This must be defensively copied, because a bolt probably has only one rotation policy object
        this.rotationPolicy = policy.copy();
        this.filePath = path;
    }

    final public long write(Tuple tuple) throws IOException{
        doWrite(tuple);
        this.needsRotation = rotationPolicy.mark(tuple, offset);

        return this.offset;
    }

    final public void sync() throws IOException {
        doSync();
    }

    final public void close() throws IOException {
        doClose();
    }

    public boolean needsRotation() {
        return needsRotation;
    }

    public Path getFilePath() {
        return this.filePath;
    }

    abstract protected void doWrite(Tuple tuple) throws IOException;

    abstract protected void doSync() throws IOException;

    abstract protected void doClose() throws IOException;

}
