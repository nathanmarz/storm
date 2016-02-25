/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.ui;

import java.util.Map;

public class FilterConfiguration {
    private String filterClass;
    private String filterName;
    private Map filterParams;


    public FilterConfiguration(Map filterParams, String filterClass) {
        this.filterParams = filterParams;
        this.filterClass = filterClass;
        this.filterName = null;
    }

    public FilterConfiguration(String filterClass, String filterName, Map filterParams) {
        this.filterClass = filterClass;
        this.filterName = filterName;
        this.filterParams = filterParams;
    }

    public String getFilterName() {
        return filterName;
    }

    public void setFilterName(String filterName) {
        this.filterName = filterName;
    }

    public String getFilterClass() {
        return filterClass;
    }

    public void setFilterClass(String filterClass) {
        this.filterClass = filterClass;
    }

    public Map getFilterParams() {
        return filterParams;
    }

    public void setFilterParams(Map filterParams) {
        this.filterParams = filterParams;
    }
}
