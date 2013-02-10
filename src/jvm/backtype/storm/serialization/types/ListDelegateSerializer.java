/**
 * Copyright (c) 2013 Yahoo! Inc. All Rights Reserved.
 *
 * Copyrights licensed under the Eclipse Public License. 
 * See the accompanying LICENSE file for terms.
 */
package backtype.storm.serialization.types;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.serializers.CollectionSerializer;
import backtype.storm.utils.ListDelegate;
import java.util.Collection;


public class ListDelegateSerializer extends CollectionSerializer {
    @Override
    public Collection create(Kryo kryo, Input input, Class<Collection> type) {
        return new ListDelegate();
    }    
}
