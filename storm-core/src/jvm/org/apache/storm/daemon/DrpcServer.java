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
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang.StringUtils;
import org.apache.storm.Config;
import org.apache.storm.daemon.metrics.MetricsUtils;
import org.apache.storm.daemon.metrics.reporters.PreparableReporter;
import org.apache.storm.generated.*;
import org.apache.storm.logging.ThriftAccessLogger;
import org.apache.storm.security.auth.*;
import org.apache.storm.security.auth.authorizer.DRPCAuthorizerBase;
import org.apache.storm.ui.FilterConfiguration;
import org.apache.storm.ui.IConfigurator;
import org.apache.storm.ui.UIHelpers;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Utils;
import org.apache.storm.utils.VersionInfo;
import org.apache.thrift.TException;
import org.eclipse.jetty.server.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.Servlet;
import java.security.Principal;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;


public class DrpcServer implements DistributedRPC.Iface, DistributedRPCInvocations.Iface, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(DrpcServer.class);
    private final Long timeoutCheckSecs = 5L;

    private Map conf;

    private ThriftServer handlerServer;
    private ThriftServer invokeServer;
    private IHttpCredentialsPlugin httpCredsHandler;

    private Thread clearThread;

    private IAuthorizer authorizer;

    //TODO: To be removed after porting drpc.clj
    private Servlet httpServlet;

    private AtomicInteger ctr = new AtomicInteger(0);
    private ConcurrentHashMap<String, ConcurrentLinkedQueue<DRPCRequest>> requestQueues = new ConcurrentHashMap<String, ConcurrentLinkedQueue<DRPCRequest>>();

    private static class InternalRequest {
        public final Semaphore sem;
        public final int startTimeSecs;
        public final String function;
        public final DRPCRequest request;
        public volatile Object result;

        public InternalRequest(String function, DRPCRequest request) {
            sem = new Semaphore(0);
            startTimeSecs = Time.currentTimeSecs();
            this.function = function;
            this.request = request;
        }
    }
    private ConcurrentHashMap<String, InternalRequest> outstandingRequests = new ConcurrentHashMap<>();


    //TODO: to be replaced by a common registry
    private final static Meter meterHttpRequests = new MetricRegistry().meter("drpc:num-execute-http-requests");
    private final static Meter meterExecuteCalls = new MetricRegistry().meter("drpc:num-execute-calls");
    private final static Meter meterResultCalls = new MetricRegistry().meter("drpc:num-result-calls");
    private final static Meter meterFailRequestCalls = new MetricRegistry().meter("drpc:num-failRequest-calls");
    private final static Meter meterFetchRequestCalls = new MetricRegistry().meter("drpc:num-fetchRequest-calls");
    private final static Meter meterShutdownCalls = new MetricRegistry().meter("drpc:num-shutdown-calls");
    
    public DrpcServer() {

    }

    //TODO: to be removed
    public void setHttpServlet(Servlet httpServlet) {
        this.httpServlet = httpServlet;
    }



    private ThriftServer initHandlerServer(Map conf, final DrpcServer service) throws Exception {
        int port = (int) conf.get(Config.DRPC_PORT);
        if (port > 0) {
            handlerServer = new ThriftServer(conf, new DistributedRPC.Processor<DistributedRPC.Iface>(service), ThriftConnectionType.DRPC);
        }
        return handlerServer;
    }

    private ThriftServer initInvokeServer(Map conf, final DrpcServer service) throws Exception {
        invokeServer = new ThriftServer(conf, new DistributedRPCInvocations.Processor<DistributedRPCInvocations.Iface>(service),
                ThriftConnectionType.DRPC_INVOCATIONS);
        return invokeServer;
    }

    private void initHttp() throws Exception{
        LOG.info("Starting  RPC Http servers...");
        Integer drpcHttpPort = (Integer) conf.get(Config.DRPC_HTTP_PORT);
        if (drpcHttpPort != null && drpcHttpPort > 0) {
            String filterClass = (String) (conf.get(Config.DRPC_HTTP_FILTER));
            Map<String, String> filterParams = (Map<String, String>) (conf.get(Config.DRPC_HTTP_FILTER_PARAMS));
            FilterConfiguration filterConfiguration = new FilterConfiguration(filterParams, filterClass);
            final List<FilterConfiguration> filterConfigurations = Arrays.asList(filterConfiguration);
            final Integer httpsPort = Utils.getInt(conf.get(Config.DRPC_HTTPS_PORT), 0);
            final String httpsKsPath = (String) (conf.get(Config.DRPC_HTTPS_KEYSTORE_PATH));
            final String httpsKsPassword = (String) (conf.get(Config.DRPC_HTTPS_KEYSTORE_PASSWORD));
            final String httpsKsType = (String) (conf.get(Config.DRPC_HTTPS_KEYSTORE_TYPE));
            final String httpsKeyPassword = (String) (conf.get(Config.DRPC_HTTPS_KEY_PASSWORD));
            final String httpsTsPath = (String) (conf.get(Config.DRPC_HTTPS_TRUSTSTORE_PATH));
            final String httpsTsPassword = (String) (conf.get(Config.DRPC_HTTPS_TRUSTSTORE_PASSWORD));
            final String httpsTsType = (String) (conf.get(Config.DRPC_HTTPS_TRUSTSTORE_TYPE));
            final Boolean httpsWantClientAuth = (Boolean) (conf.get(Config.DRPC_HTTPS_WANT_CLIENT_AUTH));
            final Boolean httpsNeedClientAuth = (Boolean) (conf.get(Config.DRPC_HTTPS_NEED_CLIENT_AUTH));

            UIHelpers.stormRunJetty(drpcHttpPort, new IConfigurator() {
                @Override
                public void execute(Server s) {
                    UIHelpers.configSsl(s, httpsPort, httpsKsPath, httpsKsPassword, httpsKsType, httpsKeyPassword, httpsTsPath, httpsTsPassword, httpsTsType,
                            httpsNeedClientAuth, httpsWantClientAuth);
                    UIHelpers.configFilter(s, httpServlet, filterConfigurations);
                }
            });
        }

    }
    private void initThrift() throws Exception {

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
        new Thread(new Runnable() {

            @Override
            public void run() {
                invokeServer.serve();
            }
        }).start();

        //TODO: To be replaced by Common.StartMetricsReporters
        List<PreparableReporter> reporters = MetricsUtils.getPreparableReporters(conf);
        for (PreparableReporter reporter : reporters) {
            reporter.prepare(new MetricRegistry(), conf);
            reporter.start();
            LOG.info("Started statistics report plugin: {}", reporter);
        }
        if (handlerServer != null)
            handlerServer.serve();
    }

    private void initClearThread() {
        clearThread = Utils.asyncLoop(new Callable() {

            @Override
            public Object call() throws Exception {
                for (Map.Entry<String, InternalRequest> e : outstandingRequests.entrySet()) {
                    InternalRequest internalRequest = e.getValue();
                    if (Time.deltaSecs(internalRequest.startTimeSecs) > Utils.getInt(conf.get(Config.DRPC_REQUEST_TIMEOUT_SECS), 0)) {
                        String id = e.getKey();
                        Semaphore sem = internalRequest.sem;
                        if (sem != null) {
                            String func = internalRequest.function;
                            acquireQueue(func).remove(internalRequest.request);
                            LOG.warn("Timeout DRPC request id: {} start at {}", id, e.getValue());
                            sem.release();
                        }
                        cleanup(id);
                        LOG.info("Clear request " + id);
                    }
                }
                return getTimeoutCheckSecs();
            }
        });
    }

    public Long getTimeoutCheckSecs() {
        return timeoutCheckSecs;
    }

    public void launchServer(boolean isLocal, Map conf) throws Exception {

        LOG.info("Starting drpc server for storm version {}", VersionInfo.getVersion());
        this.conf = conf;
        authorizer = mkAuthorizationHandler((String) (conf.get(Config.DRPC_AUTHORIZER)), conf);

        initClearThread();
        if (!isLocal){
            initThrift();
            initHttp();
        }

    }

    @Override
    public void close() {
        meterShutdownCalls.mark();
        clearThread.interrupt();
    }

    public void cleanup(String id) {
        outstandingRequests.remove(id);
    }

    @Override
    public String execute(String functionName, String funcArgs) throws DRPCExecutionException, AuthorizationException, org.apache.thrift.TException {
        meterExecuteCalls.mark();
        LOG.debug("Received DRPC request for {} ({}) at {} ", functionName, funcArgs, System.currentTimeMillis());
        Map<String, String> map = new HashMap<>();
        map.put(DRPCAuthorizerBase.FUNCTION_NAME, functionName);
        checkAuthorization(authorizer, map, "execute");

        int newid = 0;
        int orig = 0;
        do {
            orig = ctr.get();
            newid = (orig + 1) % 1000000000;
        } while (!ctr.compareAndSet(orig, newid));
        String strid = String.valueOf(newid);

        DRPCRequest req = new DRPCRequest(funcArgs, strid);
        InternalRequest internalRequest = new InternalRequest(functionName, req);
        this.outstandingRequests.put(strid, internalRequest);
        ConcurrentLinkedQueue<DRPCRequest> queue = acquireQueue(functionName);
        queue.add(req);
        LOG.debug("Waiting for DRPC request for {} {} at {}", functionName, funcArgs, System.currentTimeMillis());
        try {
            internalRequest.sem.acquire();
        } catch (InterruptedException e) {
            LOG.error("acquire fail ", e);
        }
        LOG.debug("Acquired for DRPC request for {} {} at {}", functionName, funcArgs, System.currentTimeMillis());

        Object result = internalRequest.result;

        LOG.debug("Returning for DRPC request for " + functionName + " " + funcArgs + " at " + (System.currentTimeMillis()));

        this.cleanup(strid);

        if (result instanceof DRPCExecutionException ) {
            throw (DRPCExecutionException)result;
        }
        if (result == null) {
            throw new DRPCExecutionException("Request timed out");
        }
        try {
            return String.valueOf(result);
        }catch (Exception e){
            throw new DRPCExecutionException(e.getMessage());
        }
    }

    @Override
    public void result(String id, String result) throws AuthorizationException, TException {
        meterResultCalls.mark();
        InternalRequest internalRequest = this.outstandingRequests.get(id);
        if (internalRequest != null) {
            Map<String, String> map = ImmutableMap.of(DRPCAuthorizerBase.FUNCTION_NAME, internalRequest.function);
            checkAuthorization(authorizer, map, "result");
            Semaphore sem = internalRequest.sem;
            LOG.debug("Received result {} for {} at {}", result, id, System.currentTimeMillis());
            if (sem != null) {
                internalRequest.result = result;
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
        InternalRequest internalRequest = this.outstandingRequests.get(id);
        if (internalRequest != null) {
            Map<String, String> map = new HashMap<>();
            map.put(DRPCAuthorizerBase.FUNCTION_NAME, internalRequest.function);
            checkAuthorization(authorizer, map, "failRequest");
            Semaphore sem = internalRequest.sem;
            if (sem != null) {
                internalRequest.result = new DRPCExecutionException("Request failed");
                sem.release();
            }
        }
    }

    protected ConcurrentLinkedQueue<DRPCRequest> acquireQueue(String function) {
        ConcurrentLinkedQueue<DRPCRequest> reqQueue = requestQueues.get(function);
        if (reqQueue == null) {
            reqQueue = new ConcurrentLinkedQueue<>();
            ConcurrentLinkedQueue<DRPCRequest> old = requestQueues.putIfAbsent(function, reqQueue);
            if (old != null) {
                reqQueue = old;
            }
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
}