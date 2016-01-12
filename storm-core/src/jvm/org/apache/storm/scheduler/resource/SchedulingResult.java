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

package org.apache.storm.scheduler.resource;

import org.apache.storm.scheduler.ExecutorDetails;
import org.apache.storm.scheduler.WorkerSlot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

/**
 * This class serves as a mechanism to return results and messages from a scheduling strategy to the Resource Aware Scheduler
 */
public class SchedulingResult {

    //contains the result for the attempted scheduling
    private Map<WorkerSlot, Collection<ExecutorDetails>> schedulingResultMap = null;

    //status of scheduling the topology e.g. success or fail?
    private SchedulingStatus status = null;

    //arbitrary message to be returned when scheduling is done
    private String message = null;

    //error message returned is something went wrong
    private String errorMessage = null;

    private static final Logger LOG = LoggerFactory.getLogger(SchedulingResult.class);

    private SchedulingResult(SchedulingStatus status, Map<WorkerSlot, Collection<ExecutorDetails>> schedulingResultMap, String message, String errorMessage) {
        this.status = status;
        this.schedulingResultMap = schedulingResultMap;
        this.message = message;
        this.errorMessage = errorMessage;
    }

    public static SchedulingResult failure(SchedulingStatus status, String errorMessage) {
        return new SchedulingResult(status, null, null, errorMessage);
    }

    public static SchedulingResult success(Map<WorkerSlot, Collection<ExecutorDetails>> schedulingResultMap) {
        return SchedulingResult.successWithMsg(schedulingResultMap, null);
    }

    public static SchedulingResult successWithMsg(Map<WorkerSlot, Collection<ExecutorDetails>> schedulingResultMap, String message) {
        if (schedulingResultMap == null) {
            throw new IllegalStateException("Cannot declare scheduling success without providing a non null scheduling map!");
        }
        return new SchedulingResult(SchedulingStatus.SUCCESS, schedulingResultMap, message, null);
    }

    public SchedulingStatus getStatus() {
        return this.status;
    }

    public String getMessage() {
        return this.message;
    }

    public String getErrorMessage() {
        return this.errorMessage;
    }

    public Map<WorkerSlot, Collection<ExecutorDetails>> getSchedulingResultMap() {
        return this.schedulingResultMap;
    }

    public boolean isSuccess() {
        return SchedulingStatus.isStatusSuccess(this.status);
    }

    public boolean isFailure() {
        return SchedulingStatus.isStatusFailure(this.status);
    }

    public boolean isValid() {
        if (this.isSuccess() && this.getSchedulingResultMap() == null) {
            LOG.warn("SchedulingResult not Valid! Status is success but SchedulingResultMap is null");
            return false;
        }
        if (this.isFailure() && this.getSchedulingResultMap() != null) {
            LOG.warn("SchedulingResult not Valid! Status is Failure but SchedulingResultMap is NOT null");
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        String ret = null;
        if(this.isSuccess()) {
            ret = "Status: " + this.getStatus() + " message: " + this.getMessage() + " scheduling: " + this.getSchedulingResultMap();
        } else {
            ret = "Status: " + this.getStatus() + " error message: " + this.getErrorMessage();
        }
        return ret;
    }
}
