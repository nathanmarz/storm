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

package org.apache.storm.hack;

import java.io.*;

public class IOUtil {
    public static void copy(InputStream in, OutputStream out) throws IOException {
        byte [] buffer = new byte[4096];
        int read;
        while ((read = in.read(buffer)) > 0) {
            out.write(buffer, 0, read);
        }
    }

    public static String toString(Reader reader) throws IOException {
        StringWriter ret = new StringWriter();
        char [] buffer = new char[4096];
        int read;
        while ((read = reader.read(buffer)) > 0) {
            ret.write(buffer, 0, read);
        }
        return ret.toString();
    }
}
