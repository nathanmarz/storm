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
package backtype.storm.testing;

import backtype.storm.ILocalCluster;

/**
 * This is the core interface for the storm java testing, usually
 * we put our java unit testing logic in the run method. A sample
 * code will be:
 * <code>
 * Testing.withSimulatedTimeLocalCluster(new TestJob() {
 *     public void run(Cluster cluster) {
 *         // your testing logic here.
 *     }
 * });
 */
public interface TestJob {
	/**
	 * run the testing logic with the cluster.
	 * 
	 * @param cluster the cluster which created by <code>Testing.withSimulatedTimeLocalCluster</code>
	 *        and <code>Testing.withTrackedCluster</code>.
	 */
    public void run(ILocalCluster cluster) throws Exception;
}
