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

package backtype.storm.testing;

import backtype.storm.security.auth.SimpleTransportPlugin;
import javax.security.auth.Subject;
import java.security.Principal;
import java.util.HashSet;


public class SingleUserSimpleTransport extends SimpleTransportPlugin {
   @Override
   protected Subject getDefaultSubject() {
       HashSet<Principal> principals = new HashSet<Principal>();
       principals.add(new Principal() {
          public String getName() { return "user"; }
          public String toString() { return "user"; }
       });
       return new Subject(true, principals, new HashSet<Object>(), new HashSet<Object>());
   } 
}
