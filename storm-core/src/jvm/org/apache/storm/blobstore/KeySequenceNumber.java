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

package org.apache.storm.blobstore;

import org.apache.storm.nimbus.NimbusInfo;
import org.apache.storm.utils.Utils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.TreeSet;
import java.util.Map;
import java.util.List;

/**
 * Class hands over the key sequence number which implies the number of updates made to a blob.
 * The information regarding the keys and the sequence number which represents the number of updates are
 * stored within the zookeeper in the following format.
 * /storm/blobstore/key_name/nimbushostport-sequencenumber
 * Example:
 * If there are two nimbodes with nimbus.seeds:leader,non-leader are set,
 * then the state inside the zookeeper is eventually stored as:
 * /storm/blobstore/key1/leader:8080-1
 * /storm/blobstore/key1/non-leader:8080-1
 * indicates that a new blob with the name key1 has been created on the leader
 * nimbus and the non-leader nimbus syncs after a call back is triggered by attempting
 * to download the blob and finally updates its state inside the zookeeper.
 *
 * A watch is placed on the /storm/blobstore/key1 and the znodes leader:8080-1 and
 * non-leader:8080-1 are ephemeral which implies that these nodes exist only until the
 * connection between the corresponding nimbus and the zookeeper persist. If in case the
 * nimbus crashes the node disappears under /storm/blobstore/key1.
 *
 * The sequence number for the keys are handed over based on the following scenario:
 * Lets assume there are three nimbodes up and running, one being the leader and the other
 * being the non-leader.
 *
 * 1. Create is straight forward.
 * Check whether the znode -> /storm/blobstore/key1 has been created or not. It implies
 * the blob has not been created yet. If not created, it creates it and updates the zookeeper
 * states under /storm/blobstore/key1 and /storm/blobstoremaxkeysequencenumber/key1.
 * The znodes it creates on these nodes are /storm/blobstore/key1/leader:8080-1,
 * /storm/blobstore/key1/non-leader:8080-1 and /storm/blobstoremaxkeysequencenumber/key1/1.
 * The latter holds the global sequence number across all nimbodes more like a static variable
 * indicating the true value of number of updates for a blob. This node helps to maintain sanity in case
 * leadership changes due to crashing.
 *
 * 2. Delete does not require to hand over the sequence number.
 *
 * 3. Finally, the update has few scenarios.
 *
 *  The class implements a TreeSet. The basic idea is if all the nimbodes have the same
 *  sequence number for the blob, then the number of elements in the set is 1 which holds
 *  the latest value of sequence number. If the number of elements are greater than 1 then it
 *  implies that there is sequence mismatch and there is need for syncing the blobs across
 *  nimbodes.
 *
 *  The logic for handing over sequence numbers based on the state are described as follows
 *  Here consider Nimbus-1 alias as N1 and Nimbus-2 alias as N2.
 *  Scenario 1:
 *  Example: Normal create/update scenario
 *  Operation     Nimbus-1:state     Nimbus-2:state     Seq-Num-Nimbus-1  Seq-Num-Nimbus-2          Max-Seq-Num
 *  Create-Key1   alive - Leader     alive              1                                           1
 *  Sync          alive - Leader     alive              1                 1 (callback -> download)  1
 *  Update-Key1   alive - Leader     alive              2                 1                         2
 *  Sync          alive - Leader     alive              2                 2 (callback -> download)  2
 *
 *  Scenario 2:
 *  Example: Leader nimbus crash followed by leader election, update and ex-leader restored again
 *  Operation     Nimbus-1:state     Nimbus-2:state     Seq-Num-Nimbus-1  Seq-Num-Nimbus-2          Max-Seq-Num
 *  Create        alive - Leader     alive              1                                           1
 *  Sync          alive - Leader     alive              1                 1 (callback -> download)  1
 *  Update        alive - Leader     alive              2                 1                         2
 *  Sync          alive - Leader     alive              2                 2 (callback -> download)  2
 *  Update        alive - Leader     alive              3                 2                         3
 *  Crash         crash - Leader     alive              3                 2                         3
 *  New - Leader  crash              alive - Leader     3 (Invalid)       2                         3
 *  Update        crash              alive - Leader     3 (Invalid)       4 (max-seq-num + 1)       4
 *  N1-Restored   alive              alive - Leader     0                 4                         4
 *  Sync          alive              alive - Leader     4                 4                         4
 *
 *  Scenario 3:
 *  Example: Leader nimbus crash followed by leader election, update and ex-leader restored again
 *  Operation     Nimbus-1:state     Nimbus-2:state     Seq-Num-Nimbus-1  Seq-Num-Nimbus-2          Max-Seq-Num
 *  Create        alive - Leader     alive              1                                           1
 *  Sync          alive - Leader     alive              1                 1 (callback -> download)  1
 *  Update        alive - Leader     alive              2                 1                         2
 *  Sync          alive - Leader     alive              2                 2 (callback -> download)  2
 *  Update        alive - Leader     alive              3                 2                         3
 *  Crash         crash - Leader     alive              3                 2                         3
 *  Elect Leader  crash              alive - Leader     3 (Invalid)       2                         3
 *  N1-Restored   alive              alive - Leader     3                 2                         3
 *  Read/Update   alive              alive - Leader     3                 4 (Downloads from N1)     4
 *  Sync          alive              alive - Leader     4 (callback)      4                         4
 *  Here the download is triggered whenever an operation corresponding to the blob is triggered on the
 *  nimbus like a read or update operation. Here, in the read/update call it is hard to know which call
 *  is read or update. Hence, by incrementing the sequence number to max-seq-num + 1 we ensure that the
 *  synchronization happens appropriately and all nimbodes have the same blob.
 */
public class KeySequenceNumber {
    private static final Logger LOG = LoggerFactory.getLogger(KeySequenceNumber.class);
    private final String BLOBSTORE_SUBTREE="/blobstore";
    private final String BLOBSTORE_MAX_KEY_SEQUENCE_SUBTREE="/blobstoremaxkeysequencenumber";
    private final String key;
    private final NimbusInfo nimbusInfo;
    private final int INT_CAPACITY = 4;
    private final int INITIAL_SEQUENCE_NUMBER = 1;

    public KeySequenceNumber(String key, NimbusInfo nimbusInfo) {
        this.key = key;
        this.nimbusInfo = nimbusInfo;
    }

    public synchronized int getKeySequenceNumber(Map conf) {
        TreeSet<Integer> sequenceNumbers = new TreeSet<Integer>();
        CuratorFramework zkClient = BlobStoreUtils.createZKClient(conf);
        try {
            // Key has not been created yet and it is the first time it is being created
            if(zkClient.checkExists().forPath(BLOBSTORE_SUBTREE + "/" + key) == null) {
                zkClient.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT)
                        .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE).forPath(BLOBSTORE_MAX_KEY_SEQUENCE_SUBTREE + "/" + key);
                zkClient.setData().forPath(BLOBSTORE_MAX_KEY_SEQUENCE_SUBTREE + "/" + key,
                        ByteBuffer.allocate(INT_CAPACITY).putInt(INITIAL_SEQUENCE_NUMBER).array());
                return INITIAL_SEQUENCE_NUMBER;
            }

            // When all nimbodes go down and one or few of them come up
            // Unfortunately there might not be an exact way to know which one contains the most updated blob,
            // if all go down which is unlikely. Hence there might be a need to update the blob if all go down.
            List<String> stateInfoList = zkClient.getChildren().forPath(BLOBSTORE_SUBTREE + "/" + key);
            LOG.debug("stateInfoList-size {} stateInfoList-data {}", stateInfoList.size(), stateInfoList);
            if(stateInfoList.isEmpty()) {
                return getMaxSequenceNumber(zkClient);
            }

            LOG.debug("stateInfoSize {}", stateInfoList.size());
            // In all other cases check for the latest update sequence of the blob on the nimbus
            // and assign the appropriate number. Check if all are have same sequence number,
            // if not assign the highest sequence number.
            for (String stateInfo:stateInfoList) {
                sequenceNumbers.add(Integer.parseInt(BlobStoreUtils.normalizeNimbusHostPortSequenceNumberInfo(stateInfo)
                        .getSequenceNumber()));
            }

            // Update scenario 2 and 3 explain the code logic written here
            // especially when nimbus crashes and comes up after and before update
            // respectively.
            int currentSeqNumber = getMaxSequenceNumber(zkClient);
            if (!checkIfStateContainsCurrentNimbusHost(stateInfoList, nimbusInfo) && !nimbusInfo.isLeader()) {
                if (sequenceNumbers.last() < currentSeqNumber) {
                    return currentSeqNumber;
                } else {
                    return INITIAL_SEQUENCE_NUMBER - 1;
                }
            }

            // It covers scenarios expalined in scenario 3 when nimbus-1 holding the latest
            // update goes down before it is downloaded by nimbus-2. Nimbus-2 gets elected as a leader
            // after which nimbus-1 comes back up and a read or update is performed.
            if (!checkIfStateContainsCurrentNimbusHost(stateInfoList, nimbusInfo) && nimbusInfo.isLeader()) {
                incrementMaxSequenceNumber(zkClient, currentSeqNumber);
                return currentSeqNumber + 1;
            }

            // This code logic covers the update scenarios in 2 when the nimbus-1 goes down
            // before syncing the blob to nimbus-2 and an update happens.
            // If seq-num for nimbus-2 is 2 and max-seq-number is 3 then next sequence number is 4
            // (max-seq-number + 1).
            // Other scenario it covers is when max-seq-number and nimbus seq number are equal.
            if (sequenceNumbers.size() == 1) {
                if (sequenceNumbers.first() < currentSeqNumber) {
                    incrementMaxSequenceNumber(zkClient, currentSeqNumber);
                    return currentSeqNumber + 1;
                } else {
                    incrementMaxSequenceNumber(zkClient, currentSeqNumber);
                    return sequenceNumbers.first() + 1;
                }
            }
        } catch(Exception e) {
            LOG.error("Exception {}", e);
        } finally {
            if (zkClient != null) {
                zkClient.close();
            }
        }
        // Normal create update sync scenario returns the greatest sequence number in the set
        return sequenceNumbers.last();
    }

    private boolean checkIfStateContainsCurrentNimbusHost(List<String> stateInfoList, NimbusInfo nimbusInfo) {
        boolean containsNimbusHost = false;
        for(String stateInfo:stateInfoList) {
            if(stateInfo.contains(nimbusInfo.getHost())) {
                containsNimbusHost = true;
                break;
            }
        }
        return containsNimbusHost;
    }

    private void incrementMaxSequenceNumber(CuratorFramework zkClient, int count) throws Exception {
        zkClient.setData().forPath(BLOBSTORE_MAX_KEY_SEQUENCE_SUBTREE + "/" + key,
                ByteBuffer.allocate(INT_CAPACITY).putInt(count + 1).array());
    }

    private int getMaxSequenceNumber(CuratorFramework zkClient) throws Exception {
        return ByteBuffer.wrap(zkClient.getData()
                .forPath(BLOBSTORE_MAX_KEY_SEQUENCE_SUBTREE + "/" + key)).getInt();
    }
}
