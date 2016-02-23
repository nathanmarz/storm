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
package org.apache.storm;

import org.apache.log4j.Logger;
import org.apache.storm.daemon.DrpcProcess;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.DRPCExecutionException;
import org.apache.storm.generated.DRPCRequest;
import org.apache.storm.utils.ServiceRegistry;
import org.apache.thrift.TException;

public class LocalDRPCProcess implements ILocalDRPC {
    private static final Logger LOG = Logger.getLogger(LocalDRPCProcess.class);

    private DrpcProcess handler = new DrpcProcess();
    private Thread thread;

    private final String serviceId;

    public LocalDRPCProcess() {

        thread = new Thread(new Runnable() {

            @Override
            public void run() {
                LOG.info("Begin to init local Drpc");
                try {
                    handler.launchServer();
                } catch (Exception e) {
                    LOG.info("Failed to  start local drpc");
                    System.exit(-1);
                }
                LOG.info("Successfully start local drpc");
            }
        });
        thread.start();

        serviceId = ServiceRegistry.registerService(handler);
    }

    @Override
    public String getServiceId() {
        return serviceId;
    }

    @Override
    public void result(String id, String result) throws AuthorizationException, TException {
        handler.result(id, result);
    }

    @Override
    public String execute(String functionName, String funcArgs) throws DRPCExecutionException, AuthorizationException, TException {
        return handler.execute(functionName, funcArgs);
    }

    @Override
    public void failRequest(String id) throws AuthorizationException, TException {
        handler.failRequest(id);
    }

    @Override
    public void shutdown() {
        ServiceRegistry.unregisterService(this.serviceId);
        this.handler.shutdown();
    }

    @Override
    public DRPCRequest fetchRequest(String functionName) throws AuthorizationException, TException {
        return handler.fetchRequest(functionName);
    }
}
