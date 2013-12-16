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
package storm.trident.spout;

public class RichSpoutBatchId implements IBatchID {
    long _id;    
    
    public RichSpoutBatchId(long id) {
        _id = id;
    }
    
    @Override
    public Object getId() {
        // this is to distinguish from TransactionAttempt
        return this;
    }

    @Override
    public int getAttemptId() {
        return 0; // each drpc request is always a single attempt
    }

    @Override
    public int hashCode() {
        return ((Long) _id).hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if(!(o instanceof RichSpoutBatchId)) return false;
        RichSpoutBatchId other = (RichSpoutBatchId) o;
        return _id == other._id;
    }
}
