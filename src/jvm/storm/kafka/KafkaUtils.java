package storm.kafka;

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.utils.Utils;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.List;
import kafka.api.FetchRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;
import storm.kafka.KafkaConfig.StaticHosts;

public class KafkaUtils {
    
    
     public static BatchMeta emitPartitionBatchNew(KafkaConfig config, int partition, SimpleConsumer consumer, TransactionAttempt attempt, BatchOutputCollector collector, BatchMeta lastMeta) {
         StaticHosts hosts = (StaticHosts) config.hosts;
         long offset = 0;
         if(lastMeta!=null) {
             offset = lastMeta.nextOffset;
         }
         ByteBufferMessageSet msgs;
         try {
            msgs = consumer.fetch(new FetchRequest(config.topic, partition % hosts.partitionsPerHost, offset, config.fetchSizeBytes));
         } catch(Exception e) {
             if(e instanceof ConnectException) {
                 throw new FailedFetchException(e);
             } else {
                 throw new RuntimeException(e);
             }
         }
         long endoffset = offset;
         for(MessageAndOffset msg: msgs) {
             emit(config, attempt, collector, msg.message());
             endoffset = msg.offset();
         }
         BatchMeta newMeta = new BatchMeta();
         newMeta.offset = offset;
         newMeta.nextOffset = endoffset;
         return newMeta;
     }
     
     public static void emit(KafkaConfig config, TransactionAttempt attempt, BatchOutputCollector collector, Message msg) {
         List<Object> values = config.scheme.deserialize(Utils.toByteArray(msg.payload()));
         List<Object> toEmit = new ArrayList<Object>();
         toEmit.add(attempt);
         toEmit.addAll(values);
         collector.emit(toEmit);           
     }
}
