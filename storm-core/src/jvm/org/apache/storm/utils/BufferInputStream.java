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
package org.apache.storm.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;


public class BufferInputStream {
    byte[] buffer;
    InputStream stream;

    public BufferInputStream(InputStream stream, int bufferSize) {
        this.stream = stream;
        buffer = new byte[bufferSize];
    }

    public BufferInputStream(InputStream stream) {
        this(stream, 15*1024);
    }

    public byte[] read() throws IOException {
        int length = stream.read(buffer);
        if(length==-1) {
            close();
            return new byte[0];
        } else if(length==buffer.length) {
            return buffer;
        } else {
            return Arrays.copyOf(buffer, length);
        }
    }

    public void close() throws IOException {
        stream.close();
    }
}
