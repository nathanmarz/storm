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
package storm.trident.operation.impl;

import org.apache.commons.lang.builder.ToStringBuilder;
import storm.trident.operation.TridentCollector;


//for ChainedAggregator
public class ChainedResult {
    Object[] objs;
    TridentCollector[] collectors;
    
    public ChainedResult(TridentCollector collector, int size) {
        objs = new Object[size];
        collectors = new TridentCollector[size];
        for(int i=0; i<size; i++) {
            if(size==1) {
                collectors[i] = collector;
            } else {
                collectors[i] = new CaptureCollector();                
            }
        }
    }
    
    public void setFollowThroughCollector(TridentCollector collector) {
        if(collectors.length>1) {
            for(TridentCollector c: collectors) {
                ((CaptureCollector) c).setCollector(collector);
            }
        }
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(objs);
    }    
}
