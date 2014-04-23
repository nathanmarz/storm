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
package backtype.storm.drpc;

import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.topology.ComponentConfigurationDeclarer;
import backtype.storm.tuple.Fields;

public interface LinearDRPCInputDeclarer extends ComponentConfigurationDeclarer<LinearDRPCInputDeclarer> {
    public LinearDRPCInputDeclarer fieldsGrouping(Fields fields);
    public LinearDRPCInputDeclarer fieldsGrouping(String streamId, Fields fields);

    public LinearDRPCInputDeclarer globalGrouping();
    public LinearDRPCInputDeclarer globalGrouping(String streamId);

    public LinearDRPCInputDeclarer shuffleGrouping();
    public LinearDRPCInputDeclarer shuffleGrouping(String streamId);

    public LinearDRPCInputDeclarer localOrShuffleGrouping();
    public LinearDRPCInputDeclarer localOrShuffleGrouping(String streamId);
    
    public LinearDRPCInputDeclarer noneGrouping();
    public LinearDRPCInputDeclarer noneGrouping(String streamId);

    public LinearDRPCInputDeclarer allGrouping();
    public LinearDRPCInputDeclarer allGrouping(String streamId);

    public LinearDRPCInputDeclarer directGrouping();
    public LinearDRPCInputDeclarer directGrouping(String streamId);
    
    public LinearDRPCInputDeclarer customGrouping(CustomStreamGrouping grouping);
    public LinearDRPCInputDeclarer customGrouping(String streamId, CustomStreamGrouping grouping);
    
}
