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
package storm.trident.planner;

import backtype.storm.generated.Grouping;
import backtype.storm.tuple.Fields;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import storm.trident.util.TridentUtils;


public class PartitionNode extends Node {
    public transient Grouping thriftGrouping;
    
    //has the streamid/outputFields of the node it's doing the partitioning on
    public PartitionNode(String streamId, String name, Fields allOutputFields, Grouping grouping) {
        super(streamId, name, allOutputFields);
        this.thriftGrouping = grouping;
    }
    
    private void writeObject(ObjectOutputStream oos) throws IOException {
        oos.defaultWriteObject();
        byte[] ser = TridentUtils.thriftSerialize(thriftGrouping);
        oos.writeInt(ser.length);
        oos.write(ser);
    }

    private void readObject(ObjectInputStream ois) throws ClassNotFoundException, IOException {
        ois.defaultReadObject();
        byte[] ser = new byte[ois.readInt()];
        ois.readFully(ser);
        this.thriftGrouping = TridentUtils.thriftDeserialize(Grouping.class, ser);
    }
}
