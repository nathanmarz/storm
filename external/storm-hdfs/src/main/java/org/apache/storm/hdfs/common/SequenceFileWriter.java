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
import org.apache.hadoop.io.SequenceFile;
import org.apache.storm.hdfs.bolt.format.SequenceFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class SequenceFileWriter extends AbstractHDFSWriter{

    private static final Logger LOG = LoggerFactory.getLogger(SequenceFileWriter.class);

    private SequenceFile.Writer writer;
    private SequenceFormat format;

    public SequenceFileWriter(FileRotationPolicy policy, Path path, SequenceFile.Writer writer, SequenceFormat format) {
        super(policy, path);
        this.writer = writer;
        this.format = format;
    }

    @Override
    protected void doWrite(Tuple tuple) throws IOException {
        this.writer.append(this.format.key(tuple), this.format.value(tuple));
        this.offset = this.writer.getLength();
    }

    @Override
    protected void doSync() throws IOException {
        LOG.debug("Attempting to sync all data to filesystem");
        this.writer.hsync();
    }

    @Override
    protected void doClose() throws IOException {
        this.writer.close();
    }
}
