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

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.List;
import java.util.Map;

// Todo: Track file offsets instead of line number
class TextFileReader extends AbstractFileReader {
  public static final String[] defaultFields = {"line"};
  public static final String CHARSET = "hdfsspout.reader.charset";
  public static final String BUFFER_SIZE = "hdfsspout.reader.buffer.bytes";

  private static final int DEFAULT_BUFF_SIZE = 4096;

  private BufferedReader reader;
  private final Logger LOG = LoggerFactory.getLogger(TextFileReader.class);
  private TextFileReader.Offset offset;

  public TextFileReader(FileSystem fs, Path file, Map conf) throws IOException {
    this(fs, file, conf, new TextFileReader.Offset(0,0) );
  }

  public TextFileReader(FileSystem fs, Path file, Map conf, String startOffset) throws IOException {
    this(fs, file, conf, new TextFileReader.Offset(startOffset) );
  }

  private TextFileReader(FileSystem fs, Path file, Map conf, TextFileReader.Offset startOffset)
          throws IOException {
    super(fs, file);
    offset = startOffset;
    FSDataInputStream in = fs.open(file);

    String charSet = (conf==null || !conf.containsKey(CHARSET) ) ? "UTF-8" : conf.get(CHARSET).toString();
    int buffSz = (conf==null || !conf.containsKey(BUFFER_SIZE) ) ? DEFAULT_BUFF_SIZE : Integer.parseInt( conf.get(BUFFER_SIZE).toString() );
    reader = new BufferedReader(new InputStreamReader(in, charSet), buffSz);
    if(offset.charOffset >0) {
      reader.skip(offset.charOffset);
    }

  }

  public Offset getFileOffset() {
    return offset.clone();
  }

  public List<Object> next() throws IOException, ParseException {
    String line = readLineAndTrackOffset(reader);
    if(line!=null) {
      return Collections.singletonList((Object) line);
    }
    return null;
  }

  private String readLineAndTrackOffset(BufferedReader reader) throws IOException {
    StringBuffer sb = new StringBuffer(1000);
    long before = offset.charOffset;
    int ch;
    while( (ch = reader.read()) != -1 ) {
      ++offset.charOffset;
      if (ch == '\n') {
        ++offset.lineNumber;
        return sb.toString();
      } else if( ch != '\r') {
        sb.append((char)ch);
      }
    }
    if(before==offset.charOffset) { // reached EOF, didnt read anything
      return null;
    }
    return sb.toString();
  }

  @Override
  public void close() {
    try {
      reader.close();
    } catch (IOException e) {
      LOG.warn("Ignoring error when closing file " + getFilePath(), e);
    }
  }

  public static class Offset implements FileOffset {
    long charOffset;
    long lineNumber;

    public Offset(long byteOffset, long lineNumber) {
      this.charOffset = byteOffset;
      this.lineNumber = lineNumber;
    }

    public Offset(String offset) {
      if(offset==null) {
        throw new IllegalArgumentException("offset cannot be null");
      }
      try {
        if(offset.equalsIgnoreCase("0")) {
          this.charOffset = 0;
          this.lineNumber = 0;
        } else {
          String[] parts = offset.split(":");
          this.charOffset = Long.parseLong(parts[0].split("=")[1]);
          this.lineNumber = Long.parseLong(parts[1].split("=")[1]);
        }
      } catch (Exception e) {
        throw new IllegalArgumentException("'" + offset +
                "' cannot be interpreted. It is not in expected format for TextFileReader." +
                " Format e.g.  {char=123:line=5}");
      }
    }

    @Override
    public String toString() {
      return '{' +
              "char=" + charOffset +
              ":line=" + lineNumber +
              ":}";
    }

    @Override
    public boolean isNextOffset(FileOffset rhs) {
      if(rhs instanceof Offset) {
        Offset other = ((Offset) rhs);
        return  other.charOffset > charOffset &&
                other.lineNumber == lineNumber+1;
      }
      return false;
    }

    @Override
    public int compareTo(FileOffset o) {
      Offset rhs = ((Offset)o);
      if(lineNumber < rhs.lineNumber) {
        return -1;
      }
      if(lineNumber == rhs.lineNumber) {
        return 0;
      }
      return 1;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) { return true; }
      if (!(o instanceof Offset)) { return false; }

      Offset that = (Offset) o;

      if (charOffset != that.charOffset)
        return false;
      return lineNumber == that.lineNumber;
    }

    @Override
    public int hashCode() {
      int result = (int) (charOffset ^ (charOffset >>> 32));
      result = 31 * result + (int) (lineNumber ^ (lineNumber >>> 32));
      return result;
    }

    @Override
    public Offset clone() {
      return new Offset(charOffset, lineNumber);
    }
  } //class Offset
}
