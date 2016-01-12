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

package org.apache.storm.utils;

import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;

import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZookeeperServerCnxnFactory {
	private static final Logger LOG = LoggerFactory.getLogger(ZookeeperServerCnxnFactory.class);
	int _port;
	NIOServerCnxnFactory _factory;
	
	public ZookeeperServerCnxnFactory(int port, int maxClientCnxns)  {
		//port range
		int max;
		if (port <= 0) {
			_port = 2000;
			max = 65535;
		} else {
			_port = port;
			max = port;
		}

		try {
			_factory = new NIOServerCnxnFactory();
		} catch (IOException e) {
			_port = 0;
			_factory = null;
			e.printStackTrace();
			throw new RuntimeException(e.getMessage());
		}
		
		//look for available port 
		for (; _port <= max; _port++) {
			try {
				_factory.configure(new InetSocketAddress(_port), maxClientCnxns);
				LOG.debug("Zookeeper server successfully binded at port "+_port);
				break;
			} catch (BindException e1) {
			} catch (IOException e2) {
				_port = 0;
				_factory = null;
				e2.printStackTrace();
				throw new RuntimeException(e2.getMessage());
			} 
		} 		

		if (_port > max) {
			_port = 0;
			_factory = null;
			LOG.error("Failed to find a port for Zookeeper");
			throw new RuntimeException("No port is available to launch an inprocess zookeeper.");
		}
	}
	
	public int port() {
		return _port;
	}
		
	public NIOServerCnxnFactory factory() {
		return _factory;
	}
}
