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
package storm.trident.planner;

import backtype.storm.tuple.Fields;
import java.io.Serializable;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;


public class Node implements Serializable {
    private static AtomicInteger INDEX = new AtomicInteger(0);
    
    private String nodeId;
    
    public String name = null;
    public Fields allOutputFields;
    public String streamId;
    public Integer parallelismHint = null;
    public NodeStateInfo stateInfo = null;
    public int creationIndex;
    
    public Node(String streamId, String name, Fields allOutputFields) {
        this.nodeId = UUID.randomUUID().toString();
        this.allOutputFields = allOutputFields;
        this.streamId = streamId;
        this.name = name;
        this.creationIndex = INDEX.incrementAndGet();
    }

    @Override
    public boolean equals(Object o) {
        return nodeId.equals(((Node) o).nodeId);
    }

    @Override
    public int hashCode() {
        return nodeId.hashCode();
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.MULTI_LINE_STYLE);
    }
    
    
}
