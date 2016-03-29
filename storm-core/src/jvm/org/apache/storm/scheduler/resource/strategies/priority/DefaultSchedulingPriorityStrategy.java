/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.scheduler.resource.strategies.priority;

import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.resource.SchedulingState;
import org.apache.storm.scheduler.resource.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class DefaultSchedulingPriorityStrategy implements ISchedulingPriorityStrategy {
    private static final Logger LOG = LoggerFactory
            .getLogger(DefaultSchedulingPriorityStrategy.class);

    private Cluster cluster;
    private Map<String, User> userMap;

    @Override
    public void prepare(SchedulingState schedulingState) {
        this.cluster = schedulingState.cluster;
        this.userMap = schedulingState.userMap;
    }

    @Override
    public TopologyDetails getNextTopologyToSchedule() {
        User nextUser = this.getNextUser();
        if (nextUser == null) {
            return null;
        }
        return nextUser.getNextTopologyToSchedule();
    }

    public User getNextUser() {
        Double least = Double.POSITIVE_INFINITY;
        User ret = null;
        for (User user : this.userMap.values()) {
            if (user.hasTopologyNeedSchedule()) {
                Double userResourcePoolAverageUtilization = user.getResourcePoolAverageUtilization();
                if (least > userResourcePoolAverageUtilization) {
                    ret = user;
                    least = userResourcePoolAverageUtilization;
                }
                // if ResourcePoolAverageUtilization is equal to the user that is being compared
                else if (Math.abs(least - userResourcePoolAverageUtilization) < 0.0001) {
                    double currentCpuPercentage = ret.getCPUResourceGuaranteed() / this.cluster.getClusterTotalCPUResource();
                    double currentMemoryPercentage = ret.getMemoryResourceGuaranteed() / this.cluster.getClusterTotalMemoryResource();
                    double currentAvgPercentage = (currentCpuPercentage + currentMemoryPercentage) / 2.0;

                    double userCpuPercentage = user.getCPUResourceGuaranteed() / this.cluster.getClusterTotalCPUResource();
                    double userMemoryPercentage = user.getMemoryResourceGuaranteed() / this.cluster.getClusterTotalMemoryResource();
                    double userAvgPercentage = (userCpuPercentage + userMemoryPercentage) / 2.0;
                    if (userAvgPercentage > currentAvgPercentage) {
                        ret = user;
                        least = userResourcePoolAverageUtilization;
                    }
                }
            }
        }
        return ret;
    }
}
