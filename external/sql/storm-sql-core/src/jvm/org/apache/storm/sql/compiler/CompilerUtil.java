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
package org.apache.storm.sql.compiler;

import org.apache.calcite.util.Util;

class CompilerUtil {
  static String escapeJavaString(String s, boolean nullMeansNull) {
      if(s == null) {
        return nullMeansNull ? "null" : "\"\"";
      } else {
        String s1 = Util.replace(s, "\\", "\\\\");
        String s2 = Util.replace(s1, "\"", "\\\"");
        String s3 = Util.replace(s2, "\n\r", "\\n");
        String s4 = Util.replace(s3, "\n", "\\n");
        String s5 = Util.replace(s4, "\r", "\\r");
        return "\"" + s5 + "\"";
      }
  }
}
