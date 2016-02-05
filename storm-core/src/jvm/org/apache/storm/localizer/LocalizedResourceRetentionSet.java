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

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
/**
 * A set of resources that we can look at to see which ones we retain and which ones should be
 * removed.
 */
public class LocalizedResourceRetentionSet {
  public static final Logger LOG = LoggerFactory.getLogger(LocalizedResourceRetentionSet.class);
  private long _delSize;
  private long _currentSize;
  // targetSize in Bytes
  private long _targetSize;
  private final SortedMap<LocalizedResource, LocalizedResourceSet> _noReferences;

  LocalizedResourceRetentionSet(long targetSize) {
    this(targetSize, new LRUComparator());
  }

  LocalizedResourceRetentionSet(long targetSize, Comparator<? super LocalizedResource> cmp) {
    this(targetSize, new TreeMap<LocalizedResource, LocalizedResourceSet>(cmp));
  }

  LocalizedResourceRetentionSet(long targetSize,
                                SortedMap<LocalizedResource, LocalizedResourceSet> retain) {
    this._noReferences = retain;
    this._targetSize = targetSize;
  }

  // for testing
  protected int getSizeWithNoReferences() {
    return _noReferences.size();
  }

  protected void addResourcesForSet(Iterator<LocalizedResource> setIter, LocalizedResourceSet set) {
    for (Iterator<LocalizedResource> iter = setIter; setIter.hasNext(); ) {
      LocalizedResource lrsrc = iter.next();
      _currentSize += lrsrc.getSize();
      if (lrsrc.getRefCount() > 0) {
        // always retain resources in use
        continue;
      }
      LOG.debug("adding {} to be checked for cleaning", lrsrc.getKey());
      _noReferences.put(lrsrc, set);
    }
  }

  public void addResources(LocalizedResourceSet set) {
    addResourcesForSet(set.getLocalFilesIterator(), set);
    addResourcesForSet(set.getLocalArchivesIterator(), set);
  }

  public void cleanup() {
    LOG.debug("cleanup target size: {} current size is: {}", _targetSize, _currentSize);
    for (Iterator<Map.Entry<LocalizedResource, LocalizedResourceSet>> i =
             _noReferences.entrySet().iterator();
         _currentSize - _delSize > _targetSize && i.hasNext();) {
      Map.Entry<LocalizedResource, LocalizedResourceSet> rsrc = i.next();
      LocalizedResource resource = rsrc.getKey();
      LocalizedResourceSet set = rsrc.getValue();
      if (resource != null && set.remove(resource)) {
        if (deleteResource(resource)) {
          _delSize += resource.getSize();
          LOG.info("deleting: " + resource.getFilePath() + " size of: " + resource.getSize());
          i.remove();
        } else {
          // since it failed to delete add it back so it gets retried
          set.addResource(resource.getKey(), resource, resource.isUncompressed());
        }
      }
    }
  }

  protected boolean deleteResource(LocalizedResource resource){
    try {
      String fileWithVersion = resource.getFilePathWithVersion();
      String currentSymlinkName = resource.getCurrentSymlinkPath();
      String versionFile = resource.getVersionFilePath();
      File deletePath = new File(fileWithVersion);
      if (resource.isUncompressed()) {
        // this doesn't follow symlinks, which is what we want
        FileUtils.deleteDirectory(deletePath);
      } else {
        Files.delete(deletePath.toPath());
      }
      Files.delete(new File(currentSymlinkName).toPath());
      Files.delete(new File(versionFile).toPath());
      return true;
    } catch (IOException e) {
      LOG.warn("Could not delete: {}", resource.getFilePath());
    }
    return false;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Cache: ").append(_currentSize).append(", ");
    sb.append("Deleted: ").append(_delSize);
    return sb.toString();
  }

  static class LRUComparator implements Comparator<LocalizedResource> {
    public int compare(LocalizedResource r1, LocalizedResource r2) {
      long ret = r1.getLastAccessTime() - r2.getLastAccessTime();
      if (0 == ret) {
        return System.identityHashCode(r1) - System.identityHashCode(r2);
      }
      return ret > 0 ? 1 : -1;
    }
  }
}
