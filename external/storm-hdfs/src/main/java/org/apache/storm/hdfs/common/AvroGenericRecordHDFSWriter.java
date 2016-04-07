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


import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.EnumSet;

public class AvroGenericRecordHDFSWriter extends AbstractHDFSWriter {

    private static final Logger LOG = LoggerFactory.getLogger(AvroGenericRecordHDFSWriter.class);

    private FSDataOutputStream out;
    private Schema schema;
    private DataFileWriter<GenericRecord> avroWriter;

    public AvroGenericRecordHDFSWriter(FileRotationPolicy policy, Path path, FSDataOutputStream stream, Schema schema) throws IOException {
        super(policy, path);
        this.out = stream;
        this.schema = schema;
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        avroWriter = new DataFileWriter<>(datumWriter);
        avroWriter.create(this.schema, this.out);
    }

    @Override
    protected void doWrite(Tuple tuple) throws IOException {
        GenericRecord avroRecord = (GenericRecord) tuple.getValue(0);
        avroWriter.append(avroRecord);
        offset = this.out.getPos();

        this.needsRotation = this.rotationPolicy.mark(tuple, offset);
    }

    @Override
    protected void doSync() throws IOException {
        avroWriter.flush();

        LOG.debug("Attempting to sync all data to filesystem");
        if (this.out instanceof HdfsDataOutputStream) {
            ((HdfsDataOutputStream) this.out).hsync(EnumSet.of(HdfsDataOutputStream.SyncFlag.UPDATE_LENGTH));
        } else {
            this.out.hsync();
        }
    }

    @Override
    protected void doClose() throws IOException {
        this.avroWriter.close();
        this.out.close();
    }
}
