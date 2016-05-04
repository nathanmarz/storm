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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.io.File;

import org.apache.commons.io.FileUtils;

public class VersionedStore {
    private static final String FINISHED_VERSION_SUFFIX = ".version";

    private String _root;
    
    public VersionedStore(String path) throws IOException {
      _root = path;
      mkdirs(_root);
    }

    public String getRoot() {
        return _root;
    }

    public String versionPath(long version) {
        return new File(_root, "" + version).getAbsolutePath();
    }

    public String mostRecentVersionPath() throws IOException {
        Long v = mostRecentVersion();
        if(v==null) return null;
        return versionPath(v);
    }

    public String mostRecentVersionPath(long maxVersion) throws IOException {
        Long v = mostRecentVersion(maxVersion);
        if(v==null) return null;
        return versionPath(v);
    }

    public Long mostRecentVersion() throws IOException {
        List<Long> all = getAllVersions();
        if(all.size()==0) return null;
        return all.get(0);
    }

    public Long mostRecentVersion(long maxVersion) throws IOException {
        List<Long> all = getAllVersions();
        for(Long v: all) {
            if(v <= maxVersion) return v;
        }
        return null;
    }

    public String createVersion() throws IOException {
        Long mostRecent = mostRecentVersion();
        long version = Time.currentTimeMillis();
        if(mostRecent!=null && version <= mostRecent) {
            version = mostRecent + 1;
        }
        return createVersion(version);
    }

    public String createVersion(long version) throws IOException {
        String ret = versionPath(version);
        if(getAllVersions().contains(version))
            throw new RuntimeException("Version already exists or data already exists");
        else
            return ret;
    }

    public void failVersion(String path) throws IOException {
        deleteVersion(validateAndGetVersion(path));
    }

    public void deleteVersion(long version) throws IOException {
        File versionFile = new File(versionPath(version));
        File tokenFile = new File(tokenPath(version));

        if(tokenFile.exists()) {
            FileUtils.forceDelete(tokenFile);
        }

        if(versionFile.exists()) {
            FileUtils.forceDelete(versionFile);
        }
    }

    public void succeedVersion(String path) throws IOException {
        long version = validateAndGetVersion(path);
        // should rewrite this to do a file move
        createNewFile(tokenPath(version));
    }

    public void cleanup() throws IOException {
        cleanup(-1);
    }

    public void cleanup(int versionsToKeep) throws IOException {
        List<Long> versions = getAllVersions();
        if(versionsToKeep >= 0) {
            versions = versions.subList(0, Math.min(versions.size(), versionsToKeep));
        }
        HashSet<Long> keepers = new HashSet<Long>(versions);

        for(String p: listDir(_root)) {
            Long v = parseVersion(p);
            if(v!=null && !keepers.contains(v)) {
                deleteVersion(v);
            }
        }
    }

    /**
     * Sorted from most recent to oldest
     */
    public List<Long> getAllVersions() throws IOException {
        List<Long> ret = new ArrayList<Long>();
        for(String s: listDir(_root)) {

            if(s.endsWith(FINISHED_VERSION_SUFFIX) && new File(s.substring(0, s.length() - FINISHED_VERSION_SUFFIX.length())).exists()) {
                ret.add(validateAndGetVersion(s));
            }
        }
        Collections.sort(ret);
        Collections.reverse(ret);
        return ret;
    }

    private String tokenPath(long version) {
        return new File(_root, "" + version + FINISHED_VERSION_SUFFIX).getAbsolutePath();
    }

    private long validateAndGetVersion(String path) {
        Long v = parseVersion(path);
        if(v==null) throw new RuntimeException(path + " is not a valid version");
        return v;
    }

    private Long parseVersion(String path) {
        String name = new File(path).getName();
        if(name.endsWith(FINISHED_VERSION_SUFFIX)) {
            name = name.substring(0, name.length()-FINISHED_VERSION_SUFFIX.length());
        }
        try {
            return Long.parseLong(name);
        } catch(NumberFormatException e) {
            return null;
        }
    }

    private void createNewFile(String path) throws IOException {
        new File(path).createNewFile();
    }

    private void mkdirs(String path) throws IOException {
        new File(path).mkdirs();
    }
    
    private List<String> listDir(String dir) throws IOException {
        List<String> ret = new ArrayList<String>();
        File[] contents = new File(dir).listFiles();
        if(contents!=null) {
            for(File f: contents) {
                ret.add(f.getAbsolutePath());
            }
        }
        return ret;
    }
}
