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
package org.apache.storm.localizer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Set of localized resources for a specific user.
 */
public class LocalizedResourceSet {

  public static final Logger LOG = LoggerFactory.getLogger(LocalizedResourceSet.class);
  // Key to LocalizedResource mapping for files
  private final ConcurrentMap<String, LocalizedResource> _localrsrcFiles;
  // Key to LocalizedResource mapping for files to be uncompressed
  private final ConcurrentMap<String, LocalizedResource> _localrsrcArchives;
  private String _user;

  LocalizedResourceSet(String user) {
    this._localrsrcFiles = new ConcurrentHashMap<String, LocalizedResource>();
    this._localrsrcArchives = new ConcurrentHashMap<String, LocalizedResource>();
    _user = user;
  }

  public String getUser() {
    return _user;
  }

  public int getSize() {
    return _localrsrcFiles.size() + _localrsrcArchives.size();
  }

  public LocalizedResource get(String name, boolean uncompress) {
    if (uncompress) {
      return _localrsrcArchives.get(name);
    }
    return _localrsrcFiles.get(name);
  }

  public void updateResource(String resourceName, LocalizedResource updatedResource,
                            boolean uncompress) {
    if (uncompress) {
      _localrsrcArchives.putIfAbsent(resourceName, updatedResource);
    } else {
      _localrsrcFiles.putIfAbsent(resourceName, updatedResource);
    }
  }

  public void addResource(String resourceName, LocalizedResource newResource, boolean uncompress) {
    if (uncompress) {
      _localrsrcArchives.put(resourceName, newResource);
    } else {
      _localrsrcFiles.put(resourceName, newResource);
    }
  }

  public boolean exists(String resourceName, boolean uncompress) {
    if (uncompress) {
      return (_localrsrcArchives.get(resourceName) != null);
    }
    return (_localrsrcFiles.get(resourceName) != null);
  }

  public boolean remove(LocalizedResource resource) {
    LocalizedResource lrsrc = null;
    if (resource.isUncompressed()) {
      lrsrc = _localrsrcArchives.remove(resource.getKey());
    } else {
      lrsrc = _localrsrcFiles.remove(resource.getKey());
    }
    return (lrsrc != null);
  }

  public Iterator<LocalizedResource> getLocalFilesIterator() {
    return _localrsrcFiles.values().iterator();
  }

  public Iterator<LocalizedResource> getLocalArchivesIterator() {
    return _localrsrcArchives.values().iterator();
  }
}
