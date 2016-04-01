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
package org.apache.storm.daemon.supervisor.workermanager;

import org.apache.storm.generated.WorkerResources;
import org.apache.storm.localizer.Localizer;
import org.apache.storm.utils.Utils;

import java.util.List;
import java.util.Map;

public interface IWorkerManager {
    void prepareWorker(Map conf, Localizer localizer);

    void launchWorker(String supervisorId, String assignmentId, String stormId, Long port, String workerId, WorkerResources resources,
                               Utils.ExitCodeCallable workerExitCallback);
    void shutdownWorker(String supervisorId, String workerId, Map<String, String> workerThreadPids);

    boolean cleanupWorker(String workerId);
}
