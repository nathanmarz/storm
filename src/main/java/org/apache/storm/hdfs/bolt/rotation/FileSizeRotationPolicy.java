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
package org.apache.storm.hdfs.bolt.rotation;


import backtype.storm.tuple.Tuple;

/**
 * File rotation policy that will rotate files when a certain
 * file size is reached.
 *
 * For example:
 * <pre>
 *     // rotate when files reach 5MB
 *     FileSizeRotationPolicy policy =
 *          new FileSizeRotationPolicy(5.0, Units.MB);
 * </pre>
 *
 */
public class FileSizeRotationPolicy implements FileRotationPolicy {

    public static enum Units {

        KB((long)Math.pow(2, 10)),
        MB((long)Math.pow(2, 20)),
        GB((long)Math.pow(2, 30)),
        TB((long)Math.pow(2, 40));

        private long byteCount;

        private Units(long byteCount){
            this.byteCount = byteCount;
        }

        public long getByteCount(){
            return byteCount;
        }
    }

    private long maxBytes;
    private long byteCount = 0;

    public FileSizeRotationPolicy(float count, Units units){
        this.maxBytes = (long)(count * units.getByteCount());
    }

    @Override
    public boolean mark(Tuple tuple, byte[] data) {
        this.byteCount += data.length;
        return this.byteCount >= this.maxBytes;
    }

    @Override
    public void reset() {
        this.byteCount = 0;
    }

}
