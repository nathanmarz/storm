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
package org.apache.storm.container.cgroup.core;

import org.apache.storm.container.cgroup.CgroupUtils;
import org.apache.storm.container.cgroup.SubSystemType;

import java.io.IOException;

public class FreezerCore implements CgroupCore {

    public static final String FREEZER_STATE = "/freezer.state";

    private final String dir;

    public FreezerCore(String dir) {
        this.dir = dir;
    }

    @Override
    public SubSystemType getType() {
        return SubSystemType.freezer;
    }

    public void setState(State state) throws IOException {
        CgroupUtils.writeFileByLine(CgroupUtils.getDir(this.dir, FREEZER_STATE), state.name().toUpperCase());
    }

    public State getState() throws IOException {
        return State.getStateValue(CgroupUtils.readFileByLine(CgroupUtils.getDir(this.dir, FREEZER_STATE)).get(0));
    }

    public enum State {
        frozen, freezing, thawed;

        public static State getStateValue(String state) {
            if (state.equals("FROZEN")) {
                return frozen;
            }
            else if (state.equals("FREEZING")) {
                return freezing;
            }
            else if (state.equals("THAWED")) {
                return thawed;
            }
            else {
                return null;
            }
        }
    }
}
