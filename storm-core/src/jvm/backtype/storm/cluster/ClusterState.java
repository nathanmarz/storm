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
package backtype.storm.cluster;

import clojure.lang.APersistentMap;
import clojure.lang.IFn;
import java.util.List;
import org.apache.zookeeper.data.ACL;

public interface ClusterState {
    void unregister(String id);
    void create_sequential(String path, byte[] data, List<ACL> acls);
    void mkdirs(String path, List<ACL> acls);
    void delete_node(String path);
    void set_ephemeral_node(String path, byte[] data, List<ACL> acls);
    Integer get_version(String path, boolean watch);
    boolean node_exists(String path, boolean watch);
    List<String> get_children(String path, boolean watch);
    void close();
    void set_data(String path, byte[] data, List<ACL> acls);
    String register(IFn callback);
    byte[] get_data(String path, boolean watch);
    APersistentMap get_data_with_version(String path, boolean watch);
    void set_worker_hb(String path, byte[] data, List<ACL> acls);
    byte[] get_worker_hb(String path, boolean watch);
    List<String> get_worker_hb_children(String path, boolean watch);
    void delete_worker_hb(String path);
    void add_listener(ClusterStateListener listener);
    void sync_path(String path);
}
