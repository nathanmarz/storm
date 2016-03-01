/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.sql.parser;

import com.google.common.annotations.VisibleForTesting;
import org.apache.calcite.config.Lex;
import org.apache.storm.sql.parser.impl.StormParserImpl;

import java.io.StringReader;

public class StormParser {
  public static final int DEFAULT_IDENTIFIER_MAX_LENGTH = 128;
  private final StormParserImpl impl;

  public StormParser(String s) {
    this.impl = new StormParserImpl(new StringReader(s));
    this.impl.setTabSize(1);
    this.impl.setQuotedCasing(Lex.ORACLE.quotedCasing);
    this.impl.setUnquotedCasing(Lex.ORACLE.unquotedCasing);
    this.impl.setIdentifierMaxLength(DEFAULT_IDENTIFIER_MAX_LENGTH);
    /*
     *  By default parser uses [ ] for quoting identifiers. Switching to DQID (double quoted identifiers)
     *  is needed for array and map access (m['x'] = 1 or arr[2] = 10 etc) to work.
     */
    this.impl.switchTo("DQID");
  }

  @VisibleForTesting
  public StormParserImpl impl() {
    return impl;
  }
}
