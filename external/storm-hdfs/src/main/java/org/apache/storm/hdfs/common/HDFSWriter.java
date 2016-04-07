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

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.EnumSet;

public class HDFSWriter extends AbstractHDFSWriter{

    private static final Logger LOG = LoggerFactory.getLogger(HDFSWriter.class);

    private FSDataOutputStream out;
    private RecordFormat format;

    public HDFSWriter(FileRotationPolicy policy, Path path, FSDataOutputStream out, RecordFormat format) {
        super(policy, path);
        this.out = out;
        this.format = format;
    }

    @Override
    protected void doWrite(Tuple tuple) throws IOException {
        byte[] bytes = this.format.format(tuple);
        out.write(bytes);
        this.offset += bytes.length;
    }

    @Override
    protected void doSync() throws IOException {
        LOG.info("Attempting to sync all data to filesystem");
        if (this.out instanceof HdfsDataOutputStream) {
            ((HdfsDataOutputStream) this.out).hsync(EnumSet.of(HdfsDataOutputStream.SyncFlag.UPDATE_LENGTH));
        } else {
            this.out.hsync();
        }
    }

    @Override
    protected void doClose() throws IOException {
        this.out.close();
    }
}
