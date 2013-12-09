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

/**
 * There's 3 different kinds of state:
 * 
 * 1. non-transactional: ignores commits, updates are permanent. no rollback. a cassandra incrementing state would be like this
 * 2. repeat-transactional: idempotent as long as all batches for a txid are identical
 * 3. opaque-transactional: the most general kind of state. updates are always done
 *          based on the previous version of the value if the current commit = latest stored commit
 *          Idempotent even if the batch for a txid can change.
 * 
 * repeat transactional is idempotent for transactional spouts
 * opaque transactional is idempotent for opaque or transactional spouts
 * 
 * Trident should log warnings when state is idempotent but updates will not be idempotent
 * because of spout
 */
// retrieving is encapsulated in Retrieval interface
public interface State {
    void beginCommit(Long txid); // can be null for things like partitionPersist occuring off a DRPC stream
    void commit(Long txid);
}
