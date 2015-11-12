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
package backtype.storm.codedistributor;


import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Interface responsible to distribute code in the cluster.
 */
public interface ICodeDistributor {
    /**
     * Prepare this code distributor.
     * @param conf
     */
    void prepare(Map conf) throws Exception;

    /**
     * This API will perform the actual upload of the code to the distribution implementation.
     * The API should return a Meta file which should have enough information for downloader
     * so it can download the code e.g. for bittorrent it will be a torrent file, in case of something like HDFS or s3
     * it might have the actual directory where all the code is put.
     * @param dirPath directory where all the code to be distributed exists.
     * @param topologyId the topologyId for which the meta file needs to be created.
     * @return metaFile
     */
    File upload(String dirPath, String topologyId) throws Exception;

    /**
     * Given the topologyId and metafile, download the actual code and return the downloaded file's list.
     * @param topologyid
     * @param metafile
     * @return
     */
    List<File> download(String topologyid, File metafile) throws Exception;

    /**
     * returns number of nodes to which the code is already replicated for the topology.
     * @param topologyId
     * @return
     */
    short getReplicationCount(String topologyId) throws Exception;

    /**
     * Performs the cleanup.
     * @param topologyid
     */
    void cleanup(String topologyid) throws IOException;

    /**
     * Close this distributor.
     * @param conf
     */
    void close(Map conf);
}
