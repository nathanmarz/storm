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
package org.apache.storm.security.serialization;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.BlowfishSerializer;

import org.apache.commons.codec.binary.Hex;
import org.apache.storm.Config;
import org.apache.storm.serialization.types.ListDelegateSerializer;
import org.apache.storm.utils.ListDelegate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;

/**
 * Apply Blowfish encryption for tuple communication to bolts
 */
public class BlowfishTupleSerializer extends Serializer<ListDelegate> {
    /**
     * The secret key (if any) for data encryption by blowfish payload serialization factory (BlowfishSerializationFactory). 
     * You should use in via:
     *
     * ```storm -c topology.tuple.serializer.blowfish.key=YOURKEY -c topology.tuple.serializer=org.apache.storm.security.serialization.BlowfishTupleSerializer jar ...```
     */
    public static final String SECRET_KEY = "topology.tuple.serializer.blowfish.key";
    private static final Logger LOG = LoggerFactory.getLogger(BlowfishTupleSerializer.class);
    private BlowfishSerializer _serializer;

    public BlowfishTupleSerializer(Kryo kryo, Map storm_conf) {
        String encryption_key;
        try {
            encryption_key = (String)storm_conf.get(SECRET_KEY);
            LOG.debug("Blowfish serializer being constructed ...");
            if (encryption_key == null) {
                throw new RuntimeException("Blowfish encryption key not specified");
            }
            byte[] bytes =  Hex.decodeHex(encryption_key.toCharArray());
            _serializer = new BlowfishSerializer(new ListDelegateSerializer(), bytes);
        } catch (org.apache.commons.codec.DecoderException ex) {
            throw new RuntimeException("Blowfish encryption key invalid", ex);
        }
    }

    @Override
    public void write(Kryo kryo, Output output, ListDelegate object) {
        kryo.writeObject(output, object, _serializer);
    }

    @Override
    public ListDelegate read(Kryo kryo, Input input, Class<ListDelegate> type) {
        return kryo.readObject(input, ListDelegate.class, _serializer);
    }

    /**
     * Produce a blowfish key to be used in "Storm jar" command
     */
    public static void main(String[] args) {
        try{
            KeyGenerator kgen = KeyGenerator.getInstance("Blowfish");
            SecretKey skey = kgen.generateKey();
            byte[] raw = skey.getEncoded();
            String keyString = new String(Hex.encodeHex(raw));
            System.out.println("storm -c "+SECRET_KEY+"="+keyString+" -c "+Config.TOPOLOGY_TUPLE_SERIALIZER+"="+BlowfishTupleSerializer.class.getName() + " ..." );
        } catch (Exception ex) {
            LOG.error(ex.getMessage());
            ex.printStackTrace();
        }
    }    
}
