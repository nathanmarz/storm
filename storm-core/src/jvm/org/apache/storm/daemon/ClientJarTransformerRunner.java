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

package org.apache.storm.daemon;

import org.apache.storm.utils.Utils;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.InputStream;

/**
 * Main executable to load and run a jar transformer
 */
public class ClientJarTransformerRunner {
    public static void main(String [] args) throws IOException {
        JarTransformer transformer = Utils.jarTransformer(args[0]);
        InputStream in = new FileInputStream(args[1]);
        OutputStream out = new FileOutputStream(args[2]);
        transformer.transform(in, out);
        in.close();
        out.close();
    }
}
