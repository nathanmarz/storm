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
package backtype.storm.utils;

import java.io.FilterOutputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.io.ByteArrayOutputStream;

import java.util.HashMap;

/**
 * OutputStream that escapes XML characters.  This is really slow and should only
 * be used for testing.
 */
public class XMLEscapeOutputStream extends FilterOutputStream {

    private static final HashMap<Byte, byte[]> ESCAPE_MAP = new HashMap<Byte, byte[]>();
    private static void addMapping(String key, String val) {
        ESCAPE_MAP.put(key.getBytes()[0], val.getBytes());
    }
    static {
	addMapping("\"", "&quot;");
        addMapping("'", "&apos;");
        addMapping("<", "&lt;");
        addMapping(">", "&gt;");
        addMapping("&", "&amp;");
    }

    public XMLEscapeOutputStream(OutputStream other) {
        super(other);
    }

    public void write(byte[] b) throws IOException {
        this.write(b, 0, b.length);
    }

    public void write(byte[] b, int off, int len) throws IOException {
        ByteArrayOutputStream buff = new ByteArrayOutputStream();
        for (int i = 0; i < len; i++) {
            byte[] mapped = ESCAPE_MAP.get(b[i+off]);
            if (mapped == null) {
                buff.write(b[i+off]);
            } else {
                buff.write(mapped);
            }
        }
        out.write(buff.toByteArray());
    }

    public void write(int b) throws IOException {
        byte[] mapped = ESCAPE_MAP.get((byte)b);
        if (mapped == null) {
            out.write(b);
        } else {
            out.write(mapped);
        }
    }
}
