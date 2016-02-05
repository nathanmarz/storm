/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.apache.storm.testing;

/**
 * Marker interface used to mark integration tests. Integration tests will be run during the Maven
 * <b><i>integration-test</i></b> phase, whereas unit tests will be run during the Maven <b><i>test</i></b> phase.
 * <p/>
 * Integration tests can be in the same package as unit tests. To mark a test as integration test,
 * add the annotation @Category(IntegrationTest.class) to the class definition as well as to its hierarchy of superclasses.
 * For example:
 * <p/>
 *
 *
 * @ Category(IntegrationTest.class)<br/>
 * public class MyIntegrationTest {<br/>
 *  ...<br/>
 * }
 *
 */
public interface IntegrationTest {
}
