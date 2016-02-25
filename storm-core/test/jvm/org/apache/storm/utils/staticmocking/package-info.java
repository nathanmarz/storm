/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

/**
 * Provides implementations for testing static methods.
 *
 * This package should not exist and is only necessary while we need to mock
 * static methods.
 *
 * To mock static methods in java, we use a singleton. The class to mock must
 * implement <code>setInstance</code> static method that accepts an instance of
 * the selfsame class and returns the previous instance that was set.
 *
 * Example:
 *
 * <code>
 * public class MyClass {
 *     public static MyClass setInstance(MyClass c) {
 *         MyClass oldInstance = _instance;
 *         _instance = c;
 *         return oldInstance;
 *     }
 *
 *     // Any method that we wish to mock must delegate to the singleton
 *     // instance's corresponding member method implementation
 *     public static int mockableFunction(String arg) {
 *         return _instance.mockableFunctionImpl();
 *     }
 *
 *     protected int mockableFunctionImpl(String arg) {
 *         return arg.size();
 *     }
 * }
 * </code>
 *
 * Each class that could be mocked should have an Installer class defined in
 * this package that sets the instance on construction and implements the
 * <code>close</code> method of {@link java.lang.AutoCloseable}.
 *
 * Example:
 *
 * <code>
 * class MyClassInstaller implementes AutoCloseable {
 *     private MyClass _oldInstance;
 *     private MyClass _curInstance;
 *
 *     MyClassInstaller(MyClass instance) {
 *         _oldInstance = MyClass.setInstance(instance);
 *         _curInstance = instance;
 *     }
 *
 *     @Override
 *     public void close() throws Exception {
 *        if (MyClass.setInstance(_oldInstance) != _curInstance) {
 *            throw new IllegalStateException(
 *                    "Instances of this resource must be closed in reverse order of opening.");
 *        }
 *     }
 * }
 * </code>
 *
 * To write a test with the mocked class instantiate a child class that
 * implements the close method, and use try-with-resources. For example:
 *
 * <code>
 * MyClass mock = new MyClass() {
 *         protected int mockableFunctionImpl(String arg) { return 42; }
 *         };
 *
 * try(mock) {
 *     AssertEqual(42, MyClass.mockableFunction("not 42 characters"));
 * };
 * </code>
 *
 *
 * The resulting code remains thread-unsafe.
 *
 * This class should be removed when troublesome static methods have been
 * replaced in the code.
 */
package org.apache.storm.utils.staticmocking;
