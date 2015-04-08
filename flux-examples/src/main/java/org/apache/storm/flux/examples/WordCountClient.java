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
package org.apache.storm.flux.examples;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.FileInputStream;
import java.util.Properties;

/**
 * Connects to the 'WordCount' HBase table and prints counts for each word.
 *
 * Assumes you have run (or are running) the YAML topology definition in
 * <code>simple_hbase.yaml</code>
 *
 * You will also need to modify `src/main/resources/hbase-site.xml`
 * to point to your HBase instance, and then repackage with `mvn package`.
 * This is a known issue.
 *
 */
public class WordCountClient {

    public static void main(String[] args) throws Exception {
        Configuration config = HBaseConfiguration.create();
        if(args.length == 1){
            Properties props = new Properties();
            props.load(new FileInputStream(args[0]));
            System.out.println("HBase configuration:");
            for(Object key : props.keySet()) {
                System.out.println(key + "=" + props.get(key));
                config.set((String)key, props.getProperty((String)key));
            }
        } else {
            System.out.println("Usage: WordCountClient <hbase_config.properties>");
            System.exit(1);
        }

        HTable table = new HTable(config, "WordCount");
        String[] words = new String[] {"nathan", "mike", "jackson", "golda", "bertels"};

        for (String word : words) {
            Get get = new Get(Bytes.toBytes(word));
            Result result = table.get(get);

            byte[] countBytes = result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("count"));
            byte[] wordBytes = result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("word"));

            String wordStr = Bytes.toString(wordBytes);
            long count = Bytes.toLong(countBytes);
            System.out.println("Word: '" + wordStr + "', Count: " + count);
        }

    }
}
