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
package org.apache.storm.container.cgroup;

import com.google.common.io.Files;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class CgroupUtils {

    public static final String CGROUP_STATUS_FILE = "/proc/cgroups";
    public static final String MOUNT_STATUS_FILE = "/proc/mounts";

    private static final Logger LOG = LoggerFactory.getLogger(CgroupUtils.class);

    public static void deleteDir(String dir) {
        File d = new File(dir);
        if (!d.exists()) {
            LOG.warn("dir {} does not exist!", dir);
            return;
        }
        if (!d.isDirectory()) {
            throw new RuntimeException("dir " + dir + " is not a directory!");
        }
        if (!d.delete()) {
            throw new RuntimeException("Cannot delete dir " + dir);
        }
    }

    /**
     * Get a set of SubSystemType objects from a comma delimited list of subsystem names
     */
    public static Set<SubSystemType> getSubSystemsFromString(String str) {
        Set<SubSystemType> result = new HashSet<SubSystemType>();
        String[] subSystems = str.split(",");
        for (String subSystem : subSystems) {
            //return null to mount options in string that is not part of cgroups
            SubSystemType type = SubSystemType.getSubSystem(subSystem);
            if (type != null) {
                result.add(type);
            }
        }
        return result;
    }

    /**
     * Get a string that is a comma delimited list of subsystems
     */
    public static String subSystemsToString(Set<SubSystemType> subSystems) {
        StringBuilder sb = new StringBuilder();
        if (subSystems.size() == 0) {
            return sb.toString();
        }
        for (SubSystemType type : subSystems) {
            sb.append(type.name()).append(",");
        }
        return sb.toString().substring(0, sb.length() - 1);
    }

    public static boolean enabled() {
        return Utils.checkFileExists(CGROUP_STATUS_FILE);
    }

    public static List<String> readFileByLine(String filePath) throws IOException {
        return Files.readLines(new File(filePath), Charset.defaultCharset());
    }

    public static void writeFileByLine(String filePath, List<String> linesToWrite) throws IOException {
        LOG.debug("For CGroups - writing {} to {} ", linesToWrite, filePath);
        File file = new File(filePath);
        if (!file.exists()) {
            LOG.error("{} does not exist", filePath);
            return;
        }
        try (FileWriter writer = new FileWriter(file, true);
             BufferedWriter bw = new BufferedWriter(writer)) {
            for (String string : linesToWrite) {
                bw.write(string);
                bw.newLine();
                bw.flush();
            }
        }
    }

    public static void writeFileByLine(String filePath, String lineToWrite) throws IOException {
        writeFileByLine(filePath, Arrays.asList(lineToWrite));
    }

    public static String getDir(String dir, String constant) {
        return dir + constant;
    }
}
