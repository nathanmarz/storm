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

package org.apache.storm.command;

import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TestCLI {

    @Test
    public void testSimple() throws Exception {
        Map<String, Object> values = CLI.opt("a", "aa", null)
           .opt("b", "bb", 1, CLI.AS_INT)
           .opt("c", "cc", 1, CLI.AS_INT, CLI.FIRST_WINS)
           .opt("d", "dd", null, CLI.AS_STRING, CLI.INTO_LIST)
           .opt("e", "ee", null, CLI.AS_INT)
           .opt("f", "ff", null, new PairParse(), CLI.INTO_MAP)
           .arg("A")
           .arg("B", CLI.AS_INT)
           .parse("-a100", "--aa", "200", "-c2", "-b", "50", "--cc", "100", "A-VALUE", "1", "2", "3", "-b40", "-d1", "-d2", "-d3"
                , "-f", "key1=value1", "-f", "key2=value2");
        assertEquals(8, values.size());
        assertEquals("200", (String)values.get("a"));
        assertEquals((Integer)40, (Integer)values.get("b"));
        assertEquals((Integer)2, (Integer)values.get("c"));
        assertEquals(null, values.get("e"));

        List<String> d = (List<String>)values.get("d");
        assertEquals(3, d.size());
        assertEquals("1", d.get(0));
        assertEquals("2", d.get(1));
        assertEquals("3", d.get(2));

        List<String> A = (List<String>) values.get("A");
        assertEquals(1, A.size());
        assertEquals("A-VALUE", A.get(0));

        List<Integer> B = (List<Integer>) values.get("B");
        assertEquals(3, B.size());
        assertEquals((Integer) 1, B.get(0));
        assertEquals((Integer) 2, B.get(1));
        assertEquals((Integer) 3, B.get(2));

        Map<String, String> f = (Map<String, String>) values.get("f");
        assertEquals(2, f.size());
        assertEquals("value1", f.get("key1"));
        assertEquals("value2", f.get("key2"));
    }

    private static final class PairParse implements CLI.Parse {

        @Override
        public Object parse(String value) {
            Map<String, String> result = new HashMap<>();
            String[] splits = value.split("=");
            result.put(splits[0], splits[1]);
            return result;
        }
    }
}
