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
package org.apache.storm.logging.filters;
import java.io.IOException;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AccessLoggingFilter implements Filter {

  private static final Logger LOG = LoggerFactory.getLogger(AccessLoggingFilter.class);
  public void init(FilterConfig config) throws ServletException {
  }

  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
    handle((HttpServletRequest)request, (HttpServletResponse)response, chain);
  }

  public void handle(HttpServletRequest request, HttpServletResponse response, FilterChain chain) throws IOException, ServletException{
    if (request != null) {
      LOG.info("Access from: {} url: {} principal: {}", request.getRemoteAddr(), request.getRequestURL(),
              (request.getUserPrincipal() == null ? "" : request.getUserPrincipal().getName()));
    }
    chain.doFilter(request, response);
  }

  public void destroy() {
  }
}
