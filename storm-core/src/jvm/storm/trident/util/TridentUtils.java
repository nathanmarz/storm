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
package storm.trident.util;

import backtype.storm.generated.StreamInfo;
import backtype.storm.topology.IComponent;
import backtype.storm.topology.OutputFieldsGetter;
import backtype.storm.tuple.Fields;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.jgrapht.DirectedGraph;

public class TridentUtils {
    public static Fields fieldsUnion(Fields... fields) {
        Set<String> ret = new HashSet<String>();
        for(Fields f: fields) {
            if(f!=null) ret.addAll(f.toList());
        }
        return new Fields(new ArrayList<String>(ret));
    }
    
    public static Fields fieldsConcat(Fields... fields) {
        List<String> ret = new ArrayList<String>();
        for(Fields f: fields) {
            if(f!=null) ret.addAll(f.toList());
        }
        return new Fields(ret);
    }
    
    public static Fields fieldsSubtract(Fields all, Fields minus) {
        Set<String> removeSet = new HashSet<String>(minus.toList());
        List<String> toKeep = new ArrayList<String>();
        for(String s: all.toList()) {
            if(!removeSet.contains(s)) {
                toKeep.add(s);
            }
        }
        return new Fields(toKeep);
    }
    
    public static Fields getSingleOutputStreamFields(IComponent component) {
        OutputFieldsGetter getter = new OutputFieldsGetter();
        component.declareOutputFields(getter);
        Map<String, StreamInfo> declaration = getter.getFieldsDeclaration();
        if(declaration.size()!=1) {
            throw new RuntimeException("Trident only supports components that emit a single stream");
        }
        StreamInfo si = declaration.values().iterator().next();
        if(si.is_direct()) {
            throw new RuntimeException("Trident does not support direct streams");
        }
        return new Fields(si.get_output_fields());        
    }
    
    /**
     * Assumes edge contains an index
     */
    public static <T> List<T> getParents(DirectedGraph g, T n) {
        List<IndexedEdge> incoming = new ArrayList(g.incomingEdgesOf(n));
        Collections.sort(incoming);
        List<T> ret = new ArrayList();
        for(IndexedEdge e: incoming) {
            ret.add((T)e.source);
        }        
        return ret;
    }

    public static <T> List<T> getChildren(DirectedGraph g, T n) {
        List<IndexedEdge> outgoing = new ArrayList(g.outgoingEdgesOf(n));
        List<T> ret = new ArrayList();
        for(IndexedEdge e: outgoing) {
            ret.add((T)e.target);
        }        
        return ret;
    }
    
    
    public static <T> T getParent(DirectedGraph g, T n) {
        List<T> parents = getParents(g, n);
        if(parents.size()!=1) {
            throw new RuntimeException("Expected a single parent");
        }
        return parents.get(0);
    }
    
    private static TSerializer ser = new TSerializer();
    private static TDeserializer des = new TDeserializer();
    
    public static byte[] thriftSerialize(TBase t) {
        try {
            return ser.serialize(t);
        } catch (TException e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> T thriftDeserialize(Class c, byte[] b) {
        try {
            T ret = (T) c.newInstance();
            des.deserialize((TBase) ret, b);
            return ret;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        
    }
}
