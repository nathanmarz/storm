/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.storm.hack.resource;

import org.apache.storm.hack.relocation.Relocator;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.jar.JarOutputStream;

/**
 * This is based off of
 *
 * https://github.com/apache/maven-plugins.git
 *
 * maven-shade-plugin-2.4.1
 */
public interface ResourceTransformer
{
    boolean canTransformResource( String resource );

    void processResource( String resource, InputStream is, List<Relocator> relocators )
        throws IOException;

    boolean hasTransformedResource();

    void modifyOutputStream(JarOutputStream jarOut) throws IOException;
}
