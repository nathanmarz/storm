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

package org.apache.storm.solr.config;

/**
 * Class defining a count based commit strategy. When the count reaches the commit threshold,
 * SolrInputDocuments are committed to Solr.
 */
public class CountBasedCommit implements SolrCommitStrategy {
    private int threshold;
    private int count;

    /**
     * Initializes a count based commit strategy with the specified threshold
     *
     * @param threshold  The commit threshold, defining when SolrInputDocuments should be committed to Solr
     * */
    public CountBasedCommit(int threshold) {
        if (threshold < 1) {
            throw new IllegalArgumentException("Threshold must be a positive integer: " + threshold);
        }
        this.threshold = threshold;
    }

    @Override
    public boolean commit() {
        return count != 0 && count % threshold == 0;
    }


    @Override
    public void update() {
        count++;
    }

    public int getCount() {
        return count;
    }

    public int getThreshold() {
        return threshold;
    }
}
