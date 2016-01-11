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
package org.apache.storm.testing;

import java.io.Serializable;

public class TestSerObject implements Serializable {
    public int f1;
    public int f2;
    
    public TestSerObject(int f1, int f2) {
       this.f1 = f1;
       this.f2 = f2;
    }

    @Override
    public boolean equals(Object o) {
        TestSerObject other = (TestSerObject) o;
        return f1 == other.f1 && f2 == other.f2;
    }
        
}
