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

package org.apache.storm.hack.resource;

import org.apache.storm.hack.relocation.Relocator;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.jar.JarOutputStream;
import java.util.jar.JarEntry;

public class ClojureTransformer implements ResourceTransformer {

    private final HashMap<String, String> entries = new HashMap<>();

    @Override
    public boolean canTransformResource(String s) {
        if(s.endsWith(".clj")){
            return true;
        }
        return false;
    }

    @Override
    public void processResource(String s, InputStream inputStream, List<Relocator> relocators) throws IOException {

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        int b;
        while((b = inputStream.read()) != -1){
            out.write(b);
        }
        String data = out.toString();

        for(Relocator rel : relocators){
            data = rel.applyToSourceContent(data);
        }
        this.entries.put(s, data);
    }

    @Override
    public boolean hasTransformedResource() {
        return !entries.isEmpty();
    }

    @Override
    public void modifyOutputStream(JarOutputStream jarOut) throws IOException {
        for(String key : this.entries.keySet()){
            jarOut.putNextEntry(new JarEntry(key));
            jarOut.write(this.entries.get(key).getBytes());
        }
    }
}
