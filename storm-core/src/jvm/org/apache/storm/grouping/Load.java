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
package org.apache.storm.grouping;

import java.io.Serializable;

/**
 * Represents the load that a Bolt is currently under to help in
 * deciding where to route a tuple, to help balance the load.
 */
public class Load {
    private boolean hasMetrics = false;
    private double boltLoad = 0.0; //0 no load to 1 fully loaded
    private double connectionLoad = 0.0; //0 no load to 1 fully loaded

    /**
     * Create a new load
     * @param hasMetrics have metrics been reported yet?
     * @param boltLoad the load as reported by the bolt 0.0 no load 1.0 fully loaded
     * @param connectionLoad the load as reported by the connection to the bolt 0.0 no load 1.0 fully loaded.
     */
    public Load(boolean hasMetrics, double boltLoad, double connectionLoad) {
        this.hasMetrics = hasMetrics;
        this.boltLoad = boltLoad;
        this.connectionLoad = connectionLoad;
    }

    /**
     * @return true if metrics have been reported so far.
     */
    public boolean hasMetrics() {
        return hasMetrics;
    }

    /**
     * @return the load as reported by the bolt.
     */
    public double getBoltLoad() {
        return boltLoad;
    }

    /**
     * @return the load as reported by the connection
     */
    public double getConnectionLoad() {
        return connectionLoad;
    }

    /**
     * @return the load that is a combination of sub loads.
     */
    public double getLoad() {
        if (!hasMetrics) {
            return 1.0;
        }
        return connectionLoad > boltLoad ? connectionLoad : boltLoad;
    }

    public String toString() {
        return "[:load "+boltLoad+" "+connectionLoad+"]";
    }
}
