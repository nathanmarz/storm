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
import org.apache.storm.container.cgroup.Device;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class DevicesCore implements CgroupCore {

    private final String dir;

    private static final String DEVICES_ALLOW = "/devices.allow";
    private static final String DEVICES_DENY = "/devices.deny";
    private static final String DEVICES_LIST = "/devices.list";

    private static final char TYPE_ALL = 'a';
    private static final char TYPE_BLOCK = 'b';
    private static final char TYPE_CHAR = 'c';

    private static final int ACCESS_READ = 1;
    private static final int ACCESS_WRITE = 2;
    private static final int ACCESS_CREATE = 4;

    private static final char ACCESS_READ_CH = 'r';
    private static final char ACCESS_WRITE_CH = 'w';
    private static final char ACCESS_CREATE_CH = 'm';

    private static final Logger LOG = LoggerFactory.getLogger(DevicesCore.class);

    public DevicesCore(String dir) {
        this.dir = dir;
    }

    @Override
    public SubSystemType getType() {
        return SubSystemType.devices;
    }

    public static class Record {
        Device device;
        char type;
        int accesses;

        public Record(char type, Device device, int accesses) {
            this.type = type;
            this.device = device;
            this.accesses = accesses;
        }

        public Record(String output) {
            if (output.contains("*")) {
                LOG.debug("Pre: {}", output);
                output = output.replaceAll("\\*", "-1");
                LOG.debug("After: {}",output);
            }
            String[] splits = output.split("[: ]");
            type = splits[0].charAt(0);
            int major = Integer.parseInt(splits[1]);
            int minor = Integer.parseInt(splits[2]);
            device = new Device(major, minor);
            accesses = 0;
            for (char c : splits[3].toCharArray()) {
                if (c == ACCESS_READ_CH) {
                    accesses |= ACCESS_READ;
                }
                if (c == ACCESS_CREATE_CH) {
                    accesses |= ACCESS_CREATE;
                }
                if (c == ACCESS_WRITE_CH) {
                    accesses |= ACCESS_WRITE;
                }
            }
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append(type);
            sb.append(' ');
            sb.append(device.major);
            sb.append(':');
            sb.append(device.minor);
            sb.append(' ');
            sb.append(getAccessesFlag(accesses));

            return sb.toString();
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + accesses;
            result = prime * result + ((device == null) ? 0 : device.hashCode());
            result = prime * result + type;
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Record other = (Record) obj;
            if (accesses != other.accesses) {
                return false;
            }
            if (device == null) {
                if (other.device != null) {
                    return false;
                }
            } else if (!device.equals(other.device)) {
                return false;
            }
            if (type != other.type) {
                return false;
            }
            return true;
        }

        public static Record[] parseRecordList(List<String> output) {
            Record[] records = new Record[output.size()];
            for (int i = 0, l = output.size(); i < l; i++) {
                records[i] = new Record(output.get(i));
            }

            return records;
        }

        public static StringBuilder getAccessesFlag(int accesses) {
            StringBuilder sb = new StringBuilder();
            if ((accesses & ACCESS_READ) != 0) {
                sb.append(ACCESS_READ_CH);
            }
            if ((accesses & ACCESS_WRITE) != 0) {
                sb.append(ACCESS_WRITE_CH);
            }
            if ((accesses & ACCESS_CREATE) != 0) {
                sb.append(ACCESS_CREATE_CH);
            }
            return sb;
        }
    }

    private void setPermission(String prop, char type, Device device, int accesses) throws IOException {
        Record record = new Record(type, device, accesses);
        CgroupUtils.writeFileByLine(CgroupUtils.getDir(this.dir, prop), record.toString());
    }

    public void setAllow(char type, Device device, int accesses) throws IOException {
        setPermission(DEVICES_ALLOW, type, device, accesses);
    }

    public void setDeny(char type, Device device, int accesses) throws IOException {
        setPermission(DEVICES_DENY, type, device, accesses);
    }

    public Record[] getList() throws IOException {
        List<String> output = CgroupUtils.readFileByLine(CgroupUtils.getDir(this.dir, DEVICES_LIST));
        return Record.parseRecordList(output);
    }
}
