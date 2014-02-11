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
package org.apache.storm.hdfs.trident.format;

import java.util.Map;


/**
 * Creates file names with the following format:
 * <pre>
 *     {prefix}-{partitionId}-{rotationNum}-{timestamp}{extension}
 * </pre>
 * For example:
 * <pre>
 *     MyBolt-5-7-1390579837830.txt
 * </pre>
 *
 * By default, prefix is empty and extenstion is ".txt".
 *
 */
public class DefaultFileNameFormat implements FileNameFormat {
    private int partitionIndex;
    private String path = "/storm";
    private String prefix = "";
    private String extension = ".txt";

    /**
     * Overrides the default prefix.
     *
     * @param prefix
     * @return
     */
    public DefaultFileNameFormat withPrefix(String prefix){
        this.prefix = prefix;
        return this;
    }

    /**
     * Overrides the default file extension.
     *
     * @param extension
     * @return
     */
    public DefaultFileNameFormat withExtension(String extension){
        this.extension = extension;
        return this;
    }

    public DefaultFileNameFormat withPath(String path){
        this.path = path;
        return this;
    }

    @Override
    public void prepare(Map conf, int partitionIndex, int numPartitions) {
        this.partitionIndex = partitionIndex;

    }

    @Override
    public String getName(long rotation, long timeStamp) {
        return this.prefix + "-" + this.partitionIndex +  "-" + rotation + "-" + timeStamp + this.extension;
    }

    public String getPath(){
        return this.path;
    }
}
