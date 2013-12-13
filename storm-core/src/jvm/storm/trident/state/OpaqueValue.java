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
package storm.trident.state;

import org.apache.commons.lang.builder.ToStringBuilder;

public class OpaqueValue<T> {
    Long currTxid;
    T prev;
    T curr;
    
    public OpaqueValue(Long currTxid, T val, T prev) {
        this.curr = val;
        this.currTxid = currTxid;
        this.prev = prev;
    }

    public OpaqueValue(Long currTxid, T val) {
        this(currTxid, val, null);
    }
    
    public OpaqueValue<T> update(Long batchTxid, T newVal) {
        T prev;
        if(batchTxid==null || (this.currTxid < batchTxid)) {
            prev = this.curr;
        } else if(batchTxid.equals(this.currTxid)){
            prev = this.prev;
        } else {
            throw new RuntimeException("Current batch (" + batchTxid + ") is behind state's batch: " + this.toString());
        }
        return new OpaqueValue<T>(batchTxid, newVal, prev);
    }
    
    public T get(Long batchTxid) {
        if(batchTxid==null || (this.currTxid < batchTxid)) {
            return curr;
        } else if(batchTxid.equals(this.currTxid)){
            return prev;
        } else {
            throw new RuntimeException("Current batch (" + batchTxid + ") is behind state's batch: " + this.toString());
        }
    }
    
    public T getCurr() {
        return curr;
    }
    
    public Long getCurrTxid() {
        return currTxid;
    }
    
    public T getPrev() {
        return prev;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }
}
