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
package storm.kafka;

import com.google.common.base.Objects;

import java.io.Serializable;

public class Broker implements Serializable, Comparable<Broker> {
    public final String host;
    public final int port;

    public Broker(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public Broker(String host) {
        this(host, 9092);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(host, port);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final Broker other = (Broker) obj;
        return Objects.equal(this.host, other.host) && Objects.equal(this.port, other.port);
    }

    @Override
    public String toString() {
        return host + ":" + port;
    }

    public static Broker fromString(String host) {
        Broker hp;
        String[] spec = host.split(":");
        if (spec.length == 1) {
            hp = new Broker(spec[0]);
        } else if (spec.length == 2) {
            hp = new Broker(spec[0], Integer.parseInt(spec[1]));
        } else {
            throw new IllegalArgumentException("Invalid host specification: " + host);
        }
        return hp;
    }


    @Override
    public int compareTo(Broker o) {
        if (this.host.equals(o.host)) {
            return this.port - o.port;
        } else {
            return this.host.compareTo(o.host);
        }
    }
}
