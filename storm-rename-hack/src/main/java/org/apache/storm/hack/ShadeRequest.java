/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.storm.hack;

import org.apache.storm.hack.relocation.Relocator;
import org.apache.storm.hack.resource.ResourceTransformer;

import java.util.List;

/**
 * This is based off of
 *
 * https://github.com/apache/maven-plugins.git
 *
 * maven-shade-plugin-2.4.1
 */
public class ShadeRequest
{
    private List<Relocator> relocators;

    private List<ResourceTransformer> resourceTransformers;

    public List<Relocator> getRelocators()
    {
        return relocators;
    }

    /**
     * The relocators.
     *
     * @param relocators
     */
    public void setRelocators( List<Relocator> relocators )
    {
        this.relocators = relocators;
    }

    public List<ResourceTransformer> getResourceTransformers()
    {
        return resourceTransformers;
    }

    /**
     * The transformers.
     *
     * @param resourceTransformers
     */
    public void setResourceTransformers( List<ResourceTransformer> resourceTransformers )
    {
        this.resourceTransformers = resourceTransformers;
    }
}
