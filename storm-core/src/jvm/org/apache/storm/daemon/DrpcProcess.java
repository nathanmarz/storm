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
package org.apache.storm.daemon;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import org.apache.commons.lang.StringUtils;
import org.apache.storm.Config;
import org.apache.storm.daemon.metrics.MetricsUtils;
import org.apache.storm.daemon.metrics.reporters.PreparableReporter;
import org.apache.storm.generated.*;
import org.apache.storm.logging.ThriftAccessLogger;
import org.apache.storm.security.auth.*;
import org.apache.storm.security.auth.authorizer.DRPCAuthorizerBase;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Utils;
import org.apache.storm.utils.VersionInfo;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.Principal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class DrpcProcess implements DistributedRPC.Iface, DistributedRPCInvocations.Iface, Shutdownable {

    private static final Logger LOG = LoggerFactory.getLogger(DrpcProcess.class);
    private final Integer timeoutCheckSecs = 5;

    private Map conf;

    private ThriftServer handlerServer;
    private ThriftServer invokeServer;
    private IHttpCredentialsPlugin httpCredsHandler;

    private Thread clearThread;

    private IAuthorizer authorizer;

    private AtomicInteger ctr = new AtomicInteger(0);
    private ConcurrentHashMap<String, Semaphore> idtoSem = new ConcurrentHashMap<String, Semaphore>();
    private ConcurrentHashMap<String, Object> idtoResult = new ConcurrentHashMap<String, Object>();
    private ConcurrentHashMap<String, Integer> idtoStart = new ConcurrentHashMap<String, Integer>();
    private ConcurrentHashMap<String, String> idtoFunction = new ConcurrentHashMap<String, String>();
    private ConcurrentHashMap<String, DRPCRequest> idtoRequest = new ConcurrentHashMap<String, DRPCRequest>();
    private ConcurrentHashMap<String, ConcurrentLinkedQueue<DRPCRequest>> requestQueues = new ConcurrentHashMap<String, ConcurrentLinkedQueue<DRPCRequest>>();

    private Meter meterHttpRequests = new MetricRegistry().meter("drpc:num-execute-http-requests");
    private Meter meterExecuteCalls = new MetricRegistry().meter("drpc:num-execute-calls");
    private Meter meterResultCalls = new MetricRegistry().meter("drpc:num-result-calls");
    private Meter meterFailRequestCalls = new MetricRegistry().meter("drpc:num-failRequest-calls");
    private Meter meterFetchRequestCalls = new MetricRegistry().meter("drpc:num-fetchRequest-calls");
    private Meter meterShutdownCalls = new MetricRegistry().meter("drpc:num-shutdown-calls");

    public DrpcProcess() {

    }

    private ThriftServer initHandlerServer(Map conf, final DrpcProcess service) throws Exception {
        int port = (int) conf.get(Config.DRPC_PORT);
        if (port > 0) {
            handlerServer = new ThriftServer(conf, new DistributedRPC.Processor<DistributedRPC.Iface>(service), ThriftConnectionType.DRPC);
        }
        return handlerServer;
    }

    private ThriftServer initInvokeServer(Map conf, final DrpcProcess service) throws Exception {
        invokeServer = new ThriftServer(conf, new DistributedRPCInvocations.Processor<DistributedRPCInvocations.Iface>(service),
                ThriftConnectionType.DRPC_INVOCATIONS);
        return invokeServer;
    }

    private void initServer() throws Exception {

        authorizer = mkAuthorizationHandler((String) (conf.get(Config.DRPC_AUTHORIZER)), conf);
        handlerServer = initHandlerServer(conf, this);
        invokeServer = initInvokeServer(conf, this);
        httpCredsHandler = AuthUtils.GetDrpcHttpCredentialsPlugin(conf);
        Utils.addShutdownHookWithForceKillIn1Sec(new Runnable() {
            @Override
            public void run() {
                if (handlerServer != null) {
                    handlerServer.stop();
                } else {
                    invokeServer.stop();
                }
            }
        });
        LOG.info("Starting Distributed RPC servers...");

        LOG.info("Starting Distributed RPC servers...");
        new Thread(new Runnable() {

            @Override
            public void run() {
                invokeServer.serve();
            }
        }).start();
        // To be replaced by Common.StartMetricsReporters
        List<PreparableReporter> reporters = MetricsUtils.getPreparableReporters(conf);
        for (PreparableReporter reporter : reporters) {
            reporter.prepare(new MetricRegistry(), conf);
            reporter.start();
            LOG.info("Started statistics report plugin...");
        }
        if (handlerServer != null)
            handlerServer.serve();
    }

    private void webApp(DrpcProcess drpc, IHttpCredentialsPlugin httpCredsHandler){
        meterExecuteCalls.mark();

    }
    private void initClearThread() {
        clearThread = Utils.asyncLoop(new Callable() {

            @Override
            public Object call() throws Exception {
                for (Map.Entry<String, Integer> e : idtoStart.entrySet()) {
                    if (Time.deltaSecs(e.getValue()) > (int) conf.get(Config.DRPC_REQUEST_TIMEOUT_SECS)) {
                        String id = e.getKey();
                        Semaphore sem = idtoSem.get(id);
                        if (sem != null) {
                            String func = idtoFunction.get(id);
                            acquireQueue(func).remove(idtoRequest.get(id));
                            LOG.warn("Timeout DRPC request id: {} start at {}", id, e.getValue());
                            sem.release();
                        }
                        cleanup(id);
                        LOG.info("Clear request " + id);
                    }
                }
                return timeoutCheckSecs;
            }
        });
    }

    public void launchServer() throws Exception {

        LOG.info("Starting drpc server for storm version {}", VersionInfo.getVersion());
        conf = ConfigUtils.readStormConfig();

        initClearThread();

        initServer();
    }

    @Override
    public void shutdown() {
        meterShutdownCalls.mark();
        clearThread.interrupt();
    }

    public void cleanup(String id) {
        idtoSem.remove(id);
        idtoResult.remove(id);
        idtoStart.remove(id);
        idtoFunction.remove(id);
        idtoRequest.remove(id);
    }

    @Override
    public String execute(String functionName, String funcArgs) throws DRPCExecutionException, AuthorizationException, org.apache.thrift.TException {
        meterExecuteCalls.mark();
        LOG.debug("Received DRPC request for {} {} at {} ", functionName, funcArgs, System.currentTimeMillis());
        Map<String, String> map = new HashMap<>();
        map.put(DRPCAuthorizerBase.FUNCTION_NAME, functionName);
        checkAuthorization(authorizer, map, "execute");

        int idinc = this.ctr.incrementAndGet();
        int maxvalue = 1000000000;
        int newid = idinc % maxvalue;
        if (idinc != newid) {
            this.ctr.compareAndSet(idinc, newid);
        }

        String strid = String.valueOf(newid);
        Semaphore sem = new Semaphore(0);

        DRPCRequest req = new DRPCRequest(funcArgs, strid);
        this.idtoStart.put(strid, Time.currentTimeSecs());
        this.idtoSem.put(strid, sem);
        this.idtoFunction.put(strid, functionName);
        this.idtoRequest.put(strid, req);
        ConcurrentLinkedQueue<DRPCRequest> queue = acquireQueue(functionName);
        queue.add(req);
        LOG.debug("Waiting for DRPC request for {} {} at {}", functionName, funcArgs, System.currentTimeMillis());
        try {
            sem.acquire();
        } catch (InterruptedException e) {
            LOG.error("acquire fail ", e);
        }
        LOG.debug("Acquired for DRPC request for {} {} at {}", functionName, funcArgs, System.currentTimeMillis());

        Object result = this.idtoResult.get(strid);

        LOG.info("Returning for DRPC request for " + functionName + " " + funcArgs + " at " + (System.currentTimeMillis()));

        this.cleanup(strid);

        if (result instanceof DRPCExecutionException) {
            throw (DRPCExecutionException) result;
        }
        if (result == null) {
            throw new DRPCExecutionException("Request timed out");
        }
        return String.valueOf(result);
    }

    @Override
    public void result(String id, String result) throws AuthorizationException, TException {
        meterResultCalls.mark();
        String func = this.idtoFunction.get(id);
        if (func != null) {
            Map<String, String> map = new HashMap<>();
            map.put(DRPCAuthorizerBase.FUNCTION_NAME, func);
            checkAuthorization(authorizer, map, "result");
            Semaphore sem = this.idtoSem.get(id);
            LOG.debug("Received result {} for {} at {}", result, id, System.currentTimeMillis());
            if (sem != null) {
                this.idtoResult.put(id, result);
                sem.release();
            }
        }
    }

    @Override
    public DRPCRequest fetchRequest(String functionName) throws AuthorizationException, TException {
        meterFetchRequestCalls.mark();
        Map<String, String> map = new HashMap<>();
        map.put(DRPCAuthorizerBase.FUNCTION_NAME, functionName);
        checkAuthorization(authorizer, map, "fetchRequest");
        ConcurrentLinkedQueue<DRPCRequest> queue = acquireQueue(functionName);
        DRPCRequest req = queue.poll();
        if (req != null) {
            LOG.debug("Fetched request for {} at {}", functionName, System.currentTimeMillis());
            return req;
        } else {
            return new DRPCRequest("", "");
        }
    }

    @Override
    public void failRequest(String id) throws AuthorizationException, TException {
        meterFailRequestCalls.mark();
        String func = this.idtoFunction.get(id);
        if (func != null) {
            Map<String, String> map = new HashMap<>();
            map.put(DRPCAuthorizerBase.FUNCTION_NAME, func);
            checkAuthorization(authorizer, map, "failRequest");
            Semaphore sem = this.idtoSem.get(id);
            if (sem != null) {
                this.idtoResult.put(id, new DRPCExecutionException("Request failed"));
                sem.release();
            }
        }
    }

    protected ConcurrentLinkedQueue<DRPCRequest> acquireQueue(String function) {
        ConcurrentLinkedQueue<DRPCRequest> reqQueue = requestQueues.get(function);
        if (reqQueue == null) {
            reqQueue = new ConcurrentLinkedQueue<DRPCRequest>();
            requestQueues.put(function, reqQueue);
        }
        return reqQueue;
    }

    private void checkAuthorization(IAuthorizer aclHandler, Map mapping, String operation, ReqContext reqContext) throws AuthorizationException {
        if (reqContext != null) {
            ThriftAccessLogger.logAccess(reqContext.requestID(), reqContext.remoteAddress(), reqContext.principal(), operation);
        }
        if (aclHandler != null) {
            if (reqContext == null)
                reqContext = ReqContext.context();
            if (!aclHandler.permit(reqContext, operation, mapping)) {
                Principal principal = reqContext.principal();
                String user = (principal != null) ? principal.getName() : "unknown";
                throw new AuthorizationException("DRPC request '" + operation + "' for '" + user + "' user is not authorized");
            }
        }
    }

    private void checkAuthorization(IAuthorizer aclHandler, Map mapping, String operation) throws AuthorizationException {
        checkAuthorization(aclHandler, mapping, operation, ReqContext.context());
    }

    // TO be replaced by Common.mkAuthorizationHandler
    private IAuthorizer mkAuthorizationHandler(String klassname, Map conf) {
        IAuthorizer authorizer = null;
        Class aznClass = null;
        if (StringUtils.isNotBlank(klassname)) {
            try {
                aznClass = Class.forName(klassname);
                authorizer = (IAuthorizer) aznClass.newInstance();
                if (authorizer != null) {
                    authorizer.prepare(conf);
                }
            } catch (Exception e) {
                LOG.error("mkAuthorizationHandler failed!", e);
            }
        }
        LOG.debug("authorization class name: {} class: {} handler: {}", klassname, aznClass, authorizer);
        return authorizer;
    }

    public Map getConf() {
        return conf;
    }

    public static void main(String[] args) throws Exception {

        Utils.setupDefaultUncaughtExceptionHandler();
        final DrpcProcess service = new DrpcProcess();
        service.launchServer();
    }

}