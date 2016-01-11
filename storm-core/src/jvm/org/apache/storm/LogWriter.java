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

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.InputStream;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Launch a sub process and write files out to logs.
 */
public class LogWriter extends Thread {
    private Logger logger;
    private BufferedReader in;

    public LogWriter(InputStream in, Logger logger) {
        this.in = new BufferedReader(new InputStreamReader(in));
        this.logger = logger;
    }

    public void run() {
        Logger logger = this.logger;
        BufferedReader in = this.in;
        String line;
        try {
            while ((line = in.readLine()) != null) {
                logger.info(line);
            }
        } catch (IOException e) {
            logger.error("Internal ERROR", e);
        } finally {
            try {
                in.close();
            } catch (IOException e) {
                logger.error("Internal ERROR", e);
            }
        }
    }

    public void close() throws Exception {
        this.join();
    }

    public static void main(String [] args) throws Exception {
        ProcessBuilder pb = new ProcessBuilder(args);
        Process p = pb.start();
        LogWriter err = null;
        LogWriter in = null;
        int ret = -1;
        try {
            Logger logger = LoggerFactory.getLogger("STDERR");
            err = new LogWriter(p.getErrorStream(), logger);
            err.start();
            in = new LogWriter(p.getInputStream(), logger);
            in.start();
            ret = p.waitFor();
        } finally {
          if (err != null) err.close();
          if (in != null) in.close();
        }
        System.exit(ret);
    }
}
