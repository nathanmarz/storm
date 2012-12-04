package storm.kafka.trident;

import java.net.ConnectException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.google.common.collect.ImmutableMap;

import backtype.storm.utils.Utils;
import kafka.api.FetchRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;
import storm.kafka.GlobalPartitionId;
import storm.kafka.HostPort;
import storm.kafka.KafkaConfig.StaticHosts;
import storm.kafka.KafkaConfig.ZkHosts;
import storm.trident.operation.TridentCollector;

public class KafkaUtils {
    public static IBrokerReader makeBrokerReader(Map stormConf, TridentKafkaConfig conf) {
        if(conf.hosts instanceof StaticHosts) {
            return new StaticBrokerReader((StaticHosts) conf.hosts);
        } else {
            return new ZkBrokerReader(stormConf, conf.topic, (ZkHosts) conf.hosts);
        }
    }

    public static List<GlobalPartitionId> getOrderedPartitions(Map<String, List> partitions) {
        List<GlobalPartitionId> ret = new ArrayList();
        for(String host: new TreeMap<String, List>(partitions).keySet()) {
            List info = partitions.get(host);
            long port = (Long) info.get(0);
            long numPartitions = (Long) info.get(1);
            HostPort hp = new HostPort(host, (int) port);
            for(int i=0; i<numPartitions; i++) {
                ret.add(new GlobalPartitionId(hp, i));
            }
        }
        return ret;
    }

     public static Map emitPartitionBatchNew(TridentKafkaConfig config, SimpleConsumer consumer, GlobalPartitionId partition, TridentCollector collector, Map lastMeta, String topologyInstanceId, String topologyName) {
         long offset;
         if(lastMeta!=null) {
             String lastInstanceId = null;
             Map lastTopoMeta = (Map) lastMeta.get("topology");
             if(lastTopoMeta!=null) {
                 lastInstanceId = (String) lastTopoMeta.get("id");
             }
             if(config.forceFromStart && !topologyInstanceId.equals(lastInstanceId)) {
                 offset = consumer.getOffsetsBefore(config.topic, partition.partition, config.startOffsetTime, 1)[0];
             } else {
                 offset = (Long) lastMeta.get("nextOffset");
             }
         } else {
             long startTime = -1;
             if(config.forceFromStart) startTime = config.startOffsetTime;
             offset = consumer.getOffsetsBefore(config.topic, partition.partition, startTime, 1)[0];
         }
         ByteBufferMessageSet msgs;
         try {
            msgs = consumer.fetch(new FetchRequest(config.topic, partition.partition, offset, config.fetchSizeBytes));
         } catch(Exception e) {
             if(e instanceof ConnectException) {
                 throw new FailedFetchException(e);
             } else {
                 throw new RuntimeException(e);
             }
         }
         long endoffset = offset;
         for(MessageAndOffset msg: msgs) {
             emit(config, collector, msg.message());
             endoffset = msg.offset();
         }
         Map newMeta = new HashMap();
         newMeta.put("offset", offset);
         newMeta.put("nextOffset", endoffset);
         newMeta.put("instanceId", topologyInstanceId);
         newMeta.put("partition", partition.partition);
         newMeta.put("broker", ImmutableMap.of("host", partition.host.host, "port", partition.host.port));
         newMeta.put("topic", config.topic);
         newMeta.put("topology", ImmutableMap.of("name", topologyName, "id", topologyInstanceId));
         return newMeta;
     }

     public static void emit(TridentKafkaConfig config, TridentCollector collector, Message msg) {
         Iterable<List<Object>> values =
             config.scheme.deserialize(Utils.toByteArray(msg.payload()));
         if(values!=null) {
             for(List<Object> value: values)
                 collector.emit(value);
         }
     }
}
