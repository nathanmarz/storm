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
package backtype.storm.topology;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.Grouping;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.tuple.Fields;


public interface InputDeclarer<T extends InputDeclarer> {
    public T fieldsGrouping(String componentId, Fields fields);
    public T fieldsGrouping(String componentId, String streamId, Fields fields);

    public T globalGrouping(String componentId);
    public T globalGrouping(String componentId, String streamId);

    public T shuffleGrouping(String componentId);
    public T shuffleGrouping(String componentId, String streamId);

    public T localOrShuffleGrouping(String componentId);
    public T localOrShuffleGrouping(String componentId, String streamId);

    public T noneGrouping(String componentId);
    public T noneGrouping(String componentId, String streamId);

    public T allGrouping(String componentId);
    public T allGrouping(String componentId, String streamId);

    public T directGrouping(String componentId);
    public T directGrouping(String componentId, String streamId);

    public T customGrouping(String componentId, CustomStreamGrouping grouping);
    public T customGrouping(String componentId, String streamId, CustomStreamGrouping grouping);
    
    public T grouping(GlobalStreamId id, Grouping grouping);

}
