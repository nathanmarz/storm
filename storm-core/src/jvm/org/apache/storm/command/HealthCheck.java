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
package org.apache.storm.command;

import org.apache.storm.Config;
import org.apache.storm.utils.ConfigUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class HealthCheck {

    private static final Logger LOG = LoggerFactory.getLogger(HealthCheck.class);
    private static final String FAILED = "failed";
    private static final String SUCCESS = "success";
    private static final String TIMEOUT = "timeout";
    private static final String FAILED_WITH_EXIT_CODE = "failed_with_exit_code";

    public static int healthCheck(Map conf) {
        String healthDir = ConfigUtils.absoluteHealthCheckDir(conf);
        List<String> results = new ArrayList<>();
        if (healthDir != null) {
            File parentFile = new File(healthDir);
            List<String> healthScripts = new ArrayList<String>();
            if (parentFile.exists()) {
                File[] list = parentFile.listFiles();
                for (File f : list) {
                    if (!f.isDirectory() && f.canExecute())
                        healthScripts.add(f.getAbsolutePath());
                }
            }
            for (String script : healthScripts) {
                String result = processScript(conf, script);
                results.add(result);
            }
        }

        // failed_with_exit_code is OK. We're mimicing Hadoop's health checks.
        // We treat non-zero exit codes as indicators that the scripts failed
        // to execute properly, not that the system is unhealthy, in which case
        // we don't want to start killing things.

        if (results.contains(FAILED) || results.contains(TIMEOUT)) {
            return 1;
        } else {
            return 0;
        }

    }

    public static String processScript(Map conf, String script) {
        Thread interruptThread = null;
        try {
            Process process = Runtime.getRuntime().exec(script);
            final long timeout = (long) (conf.get(Config.STORM_HEALTH_CHECK_TIMEOUT_MS));
            final Thread curThread = Thread.currentThread();
            // kill process when timeout
            interruptThread = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        Thread.sleep(timeout);
                        curThread.interrupt();
                    } catch (InterruptedException e) {
                        // Ignored
                    }
                }
            });
            interruptThread.start();
            process.waitFor();
            interruptThread.interrupt();
            curThread.interrupted();

            if (process.exitValue() != 0) {
                String str;
                InputStream stdin = process.getInputStream();
                BufferedReader reader = new BufferedReader(new InputStreamReader(stdin));
                while ((str = reader.readLine()) != null) {
                    if (str.startsWith("ERROR")) {
                        return FAILED;
                    }
                }
                return SUCCESS;
            }
            return FAILED_WITH_EXIT_CODE;
        } catch (InterruptedException e) {
            LOG.warn("Script:  {} timed out.", script);
            return TIMEOUT;
        } catch (Exception e) {
            LOG.warn("Script failed with exception: ", e);
            return FAILED_WITH_EXIT_CODE;
        } finally {
            if (interruptThread != null)
                interruptThread.interrupt();
        }
    }

    public static void main(String[] args) {
        Map conf = ConfigUtils.readStormConfig();
        System.exit(healthCheck(conf));
    }

}