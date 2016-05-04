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

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class LocalizedResourceRetentionSetTest {

  @Test
  public void testAddResources() throws Exception {
    LocalizedResourceRetentionSet lrretset = new LocalizedResourceRetentionSet(10);
    LocalizedResourceSet lrset = new LocalizedResourceSet("user1");
    LocalizedResource localresource1 = new LocalizedResource("key1", "testfile1", false, "topo1");
    LocalizedResource localresource2 = new LocalizedResource("key2", "testfile2", false, "topo1");
    // check adding reference to local resource with topology of same name
    localresource2.addReference(("topo2"));

    lrset.addResource("key1", localresource1, false);
    lrset.addResource("key2", localresource2, false);
    lrretset.addResources(lrset);
    assertEquals("number to clean is not 0", 0, lrretset.getSizeWithNoReferences());
    localresource1.removeReference(("topo1"));
    lrretset.addResources(lrset);
    assertEquals("number to clean is not 1", 1, lrretset.getSizeWithNoReferences());
    localresource2.removeReference(("topo1"));
    lrretset.addResources(lrset);
    assertEquals("number to clean is not 1", 1, lrretset.getSizeWithNoReferences());
    localresource2.removeReference(("topo2"));
    lrretset.addResources(lrset);
    assertEquals("number to clean is not 2", 2, lrretset.getSizeWithNoReferences());
  }

  @Test
  public void testCleanup() throws Exception {
    LocalizedResourceRetentionSet lrretset = spy(new LocalizedResourceRetentionSet(10));
    LocalizedResourceSet lrset = new LocalizedResourceSet("user1");
    // no reference to key1
    LocalizedResource localresource1 = new LocalizedResource("key1", "testfile1", false);
    localresource1.setSize(10);
    // no reference to archive1
    LocalizedResource archiveresource1 = new LocalizedResource("archive1", "testarchive1", true);
    archiveresource1.setSize(20);
    // reference to key2
    LocalizedResource localresource2 = new LocalizedResource("key2", "testfile2", false, "topo1");
    // check adding reference to local resource with topology of same name
    localresource2.addReference(("topo1"));
    localresource2.setSize(10);
    lrset.addResource("key1", localresource1, false);
    lrset.addResource("key2", localresource2, false);
    lrset.addResource("archive1", archiveresource1, true);

    lrretset.addResources(lrset);
    assertEquals("number to clean is not 2", 2, lrretset.getSizeWithNoReferences());

    // shouldn't change number since file doesn't exist and delete fails
    lrretset.cleanup();
    assertEquals("resource cleaned up", 2, lrretset.getSizeWithNoReferences());

    // make deleteResource return true even though file doesn't exist
    when(lrretset.deleteResource(localresource1)).thenReturn(true);
    when(lrretset.deleteResource(localresource2)).thenReturn(true);
    when(lrretset.deleteResource(archiveresource1)).thenReturn(true);
    lrretset.cleanup();
    assertEquals("resource not cleaned up", 0, lrretset.getSizeWithNoReferences());
  }
}
