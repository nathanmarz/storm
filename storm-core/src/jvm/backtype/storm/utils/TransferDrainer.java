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
package backtype.storm.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.TaskMessage;

public class TransferDrainer {

  private HashMap<String, ArrayList<ArrayList<TaskMessage>>> bundles = new HashMap();
  
  public void add(HashMap<String, ArrayList<TaskMessage>> workerTupleSetMap) {
    for (String key : workerTupleSetMap.keySet()) {
      
      ArrayList<ArrayList<TaskMessage>> bundle = bundles.get(key);
      if (null == bundle) {
        bundle = new ArrayList<ArrayList<TaskMessage>>();
        bundles.put(key, bundle);
      }
      
      ArrayList tupleSet = workerTupleSetMap.get(key);
      if (null != tupleSet && tupleSet.size() > 0) {
        bundle.add(tupleSet);
      }
    } 
  }
  
  public void send(HashMap<String, IConnection> connections) {
    for (String hostPort : bundles.keySet()) {
      IConnection connection = connections.get(hostPort);
      if (null != connection) { 
        ArrayList<ArrayList<TaskMessage>> bundle = bundles.get(hostPort);
        Iterator<TaskMessage> iter = getBundleIterator(bundle);
        if (null != iter) {
          connection.send(iter);
        }
      }
    } 
  }
  
  private Iterator<TaskMessage> getBundleIterator(final ArrayList<ArrayList<TaskMessage>> bundle) {
    
    if (null == bundle) {
      return null;
    }
    
    return new Iterator<TaskMessage> () {
      
      private int offset = 0;
      private int size = 0;
      {
        for (ArrayList<TaskMessage> list : bundle) {
            size += list.size();
        }
      }
      
      private int bundleOffset = 0;
      private Iterator<TaskMessage> iter = bundle.get(bundleOffset).iterator();
      
      @Override
      public boolean hasNext() {
        if (offset < size) {
          return true;
        }
        return false;
      }

      @Override
      public TaskMessage next() {
        TaskMessage msg = null;
        if (iter.hasNext()) {
          msg = iter.next(); 
        } else {
          bundleOffset++;
          iter = bundle.get(bundleOffset).iterator();
          msg = iter.next();
        }
        if (null != msg) {
          offset++;
        }
        return msg;
      }

      @Override
      public void remove() {
        throw new RuntimeException("not supported");
      }
    };
  }
  
  public void clear() {
    bundles.clear();
  }
}