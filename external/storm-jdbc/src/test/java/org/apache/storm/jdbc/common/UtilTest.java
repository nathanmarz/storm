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
package org.apache.storm.jdbc.common;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import org.junit.Test;

import static org.junit.Assert.*;

public class UtilTest {

    @Test
    public void testBasic() {
        assertEquals(String.class, Util.getJavaType(Types.CHAR));
        assertEquals(String.class, Util.getJavaType(Types.VARCHAR));
        assertEquals(String.class, Util.getJavaType(Types.LONGVARCHAR));
        assertEquals(byte[].class, Util.getJavaType(Types.BINARY));
        assertEquals(byte[].class, Util.getJavaType(Types.VARBINARY));
        assertEquals(byte[].class, Util.getJavaType(Types.LONGVARBINARY));
        assertEquals(Boolean.class, Util.getJavaType(Types.BIT));
        assertEquals(Short.class, Util.getJavaType(Types.TINYINT));
        assertEquals(Short.class, Util.getJavaType(Types.SMALLINT));
        assertEquals(Integer.class, Util.getJavaType(Types.INTEGER));
        assertEquals(Long.class, Util.getJavaType(Types.BIGINT));
        assertEquals(Float.class, Util.getJavaType(Types.REAL));
        assertEquals(Double.class, Util.getJavaType(Types.DOUBLE));
        assertEquals(Double.class, Util.getJavaType(Types.FLOAT));
        assertEquals(Date.class, Util.getJavaType(Types.DATE));
        assertEquals(Time.class, Util.getJavaType(Types.TIME));
        assertEquals(Timestamp.class, Util.getJavaType(Types.TIMESTAMP));
    }

    @Test
    public void testError() {
        //This test is rather ugly, but it is the only way to see if the error messages are working correctly.
        try {
            Util.getJavaType(Types.REF);
            fail("didn't throw like expected");
        } catch (Exception e) {
            assertEquals("We do not support tables with SqlType: REF", e.getMessage());
        }

        try {
            Util.getJavaType(-1000);
            fail("didn't throw like expected");
        } catch (Exception e) {
            assertEquals("Unknown sqlType -1000", e.getMessage());
        }

    }
}
