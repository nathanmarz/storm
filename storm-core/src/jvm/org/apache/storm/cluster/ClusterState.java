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
package org.apache.storm.cluster;

import clojure.lang.APersistentMap;
import clojure.lang.IFn;
import java.util.List;
import org.apache.zookeeper.data.ACL;

/**
 * ClusterState provides the API for the pluggable state store used by the
 * Storm daemons. Data is stored in path/value format, and the store supports
 * listing sub-paths at a given path.
 * All data should be available across all nodes with eventual consistency.
 *
 * IMPORTANT NOTE:
 * Heartbeats have different api calls used to interact with them. The root
 * path (/) may or may not be the same as the root path for the other api calls.
 *
 * For example, performing these two calls:
 *     set_data("/path", data, acls);
 *     void set_worker_hb("/path", heartbeat, acls);
 * may or may not cause a collision in "/path".
 * Never use the same paths with the *_hb* methods as you do with the others.
 */
public interface ClusterState {

    /**
     * Registers a callback function that gets called when CuratorEvents happen.
     * @param callback is a clojure IFn that accepts the type - translated to
     * clojure keyword as in zookeeper.clj - and the path: (callback type path)
     * @return is an id that can be passed to unregister(...) to unregister the
     * callback.
     */
    String register(IFn callback);

    /**
     * Unregisters a callback function that was registered with register(...).
     * @param id is the String id that was returned from register(...).
     */
    void unregister(String id);

    /**
     * Path will be appended with a monotonically increasing integer, a new node
     * will be created there, and data will be put at that node.
     * @param path The path that the monotonically increasing integer suffix will
     * be added to.
     * @param data The data that will be written at the suffixed path's node.
     * @param acls The acls to apply to the path. May be null.
     * @return The path with the integer suffix appended.
     */
    String create_sequential(String path, byte[] data, List<ACL> acls);

    /**
     * Creates nodes for path and all its parents. Path elements are separated by
     * a "/", as in *nix filesystem notation. Equivalent to mkdir -p in *nix.
     * @param path The path to create, along with all its parents.
     * @param acls The acls to apply to the path. May be null.
     * @return path
     */
    String mkdirs(String path, List<ACL> acls);

    /**
     * Deletes the node at a given path, and any child nodes that may exist.
     * @param path The path to delete
     */
    void delete_node(String path);

    /**
     * Creates an ephemeral node at path. Ephemeral nodes are destroyed
     * by the store when the client disconnects.
     * @param path The path where a node will be created.
     * @param data The data to be written at the node.
     * @param acls The acls to apply to the path. May be null.
     */
    void set_ephemeral_node(String path, byte[] data, List<ACL> acls);

    /**
     * Gets the 'version' of the node at a path. Optionally sets a watch
     * on that node. The version should increase whenever a write happens.
     * @param path The path to get the version of.
     * @param watch Whether or not to set a watch on the path. Watched paths
     * emit events which are consumed by functions registered with the
     * register method. Very useful for catching updates to nodes.
     * @return The integer version of this node.
     */
    Integer get_version(String path, boolean watch);

    /**
     * Check if a node exists and optionally set a watch on the path.
     * @param path The path to check for the existence of a node.
     * @param watch Whether or not to set a watch on the path. Watched paths
     * emit events which are consumed by functions registered with the
     * register method. Very useful for catching updates to nodes.
     * @return Whether or not a node exists at path.
     */
    boolean node_exists(String path, boolean watch);

    /**
     * Get a list of paths of all the child nodes which exist immediately
     * under path.
     * @param path The path to look under
     * @param watch Whether or not to set a watch on the path. Watched paths
     * emit events which are consumed by functions registered with the
     * register method. Very useful for catching updates to nodes.
     * @return list of string paths under path.
     */
    List<String> get_children(String path, boolean watch);

    /**
     * Close the connection to the data store.
     */
    void close();

    /**
     * Set the value of the node at path to data.
     * @param path The path whose node we want to set.
     * @param data The data to put in the node.
     * @param acls The acls to apply to the path. May be null.
     */
    void set_data(String path, byte[] data, List<ACL> acls);

    /**
     * Get the data from the node at path
     * @param path The path to look under
     * @param watch Whether or not to set a watch on the path. Watched paths
     * emit events which are consumed by functions registered with the
     * register method. Very useful for catching updates to nodes.
     * @return The data at the node.
     */
    byte[] get_data(String path, boolean watch);

    /**
     * Get the data at the node along with its version. Data is returned
     * in an APersistentMap with clojure keyword keys :data and :version.
     * @param path The path to look under
     * @param watch Whether or not to set a watch on the path. Watched paths
     * emit events which are consumed by functions registered with the
     * register method. Very useful for catching updates to nodes.
     * @return An APersistentMap in the form {:data data :version version}
     */
    APersistentMap get_data_with_version(String path, boolean watch);

    /**
     * Write a worker heartbeat at the path.
     * @param path The path whose node we want to set.
     * @param data The data to put in the node.
     * @param acls The acls to apply to the path. May be null.
     */
    void set_worker_hb(String path, byte[] data, List<ACL> acls);

    /**
     * Get the heartbeat from the node at path
     * @param path The path to look under
     * @param watch Whether or not to set a watch on the path. Watched paths
     * emit events which are consumed by functions registered with the
     * register method. Very useful for catching updates to nodes.
     * @return The heartbeat at the node.
     */
    byte[] get_worker_hb(String path, boolean watch);

    /**
     * Get a list of paths of all the child nodes which exist immediately
     * under path. This is similar to get_children, but must be used for
     * any nodes
     * @param path The path to look under
     * @param watch Whether or not to set a watch on the path. Watched paths
     * emit events which are consumed by functions registered with the
     * register method. Very useful for catching updates to nodes.
     * @return list of string paths under path.
     */
    List<String> get_worker_hb_children(String path, boolean watch);

    /**
     * Deletes the heartbeat at a given path, and any child nodes that may exist.
     * @param path The path to delete.
     */
    void delete_worker_hb(String path);

    /**
     * Add a ClusterStateListener to the connection.
     * @param listener A ClusterStateListener to handle changing cluster state
     * events.
     */
    void add_listener(ClusterStateListener listener);

    /**
     * Force consistency on a path. Any writes committed on the path before
     * this call will be completely propagated when it returns.
     * @param path The path to synchronize.
     */
    void sync_path(String path);

    /**
     * Allows us to delete the znodes within /storm/blobstore/key_name
     * whose znodes start with the corresponding nimbusHostPortInfo
     * @param path /storm/blobstore/key_name
     * @param nimbusHostPortInfo Contains the host port information of
     * a nimbus node.
     */
    void delete_node_blobstore(String path, String nimbusHostPortInfo);
}
