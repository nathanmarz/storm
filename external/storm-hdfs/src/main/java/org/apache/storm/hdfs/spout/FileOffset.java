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

package org.apache.storm.hdfs.spout;

/**
 * Represents the notion of an offset in a file. Idea is accommodate representing file
 * offsets other than simple byte offset as it may be insufficient for certain formats.
 * Reader for each format implements this as appropriate for its needs.
 * Note: Derived types must :
 *       - implement equals() & hashCode() appropriately.
 *       - implement Comparable<> appropriately.
 *       - implement toString() appropriately for serialization.
 *       - constructor(string) for deserialization
 */

interface FileOffset extends Comparable<FileOffset>, Cloneable {
  /** tests if rhs == currOffset+1 */
  boolean isNextOffset(FileOffset rhs);
  FileOffset clone();
}