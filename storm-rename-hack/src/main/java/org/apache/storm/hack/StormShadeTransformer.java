/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.hack;

import org.apache.storm.daemon.JarTransformer;
import org.apache.storm.hack.relocation.Relocator;
import org.apache.storm.hack.relocation.SimpleRelocator;
import org.apache.storm.hack.resource.ClojureTransformer;
import org.apache.storm.hack.resource.ResourceTransformer;

import java.io.*;
import java.util.Arrays;

public class StormShadeTransformer implements JarTransformer {
    @Override
    public void transform(InputStream input, OutputStream output) throws IOException {
        DefaultShader shader = new DefaultShader();
        ShadeRequest request = StormShadeRequest.makeRequest();
        shader.shadeJarStream(request,input, output);
    }
}
