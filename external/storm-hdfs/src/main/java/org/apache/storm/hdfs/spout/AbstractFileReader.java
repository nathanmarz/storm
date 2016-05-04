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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


abstract class AbstractFileReader implements FileReader {

  private final Path file;

  public AbstractFileReader(FileSystem fs, Path file) {
    if (fs == null ) {
      throw new IllegalArgumentException("filesystem arg cannot be null for reader");
    }
    if (file == null ) {
      throw new IllegalArgumentException("file arg cannot be null for reader");
    }
    this.file = file;
  }

  @Override
  public Path getFilePath() {
    return file;
  }


  @Override
  public boolean equals(Object o) {
    if (this == o) { return true; }
    if (o == null || getClass() != o.getClass()) { return false; }

    AbstractFileReader that = (AbstractFileReader) o;

    return !(file != null ? !file.equals(that.file) : that.file != null);
  }

  @Override
  public int hashCode() {
    return file != null ? file.hashCode() : 0;
  }

}
