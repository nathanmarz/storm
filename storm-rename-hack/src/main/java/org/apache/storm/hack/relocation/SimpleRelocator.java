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

package org.apache.storm.hack.relocation;

/**
 * This is based off of
 *
 * https://github.com/apache/maven-plugins.git
 *
 * maven-shade-plugin-2.4.1
 */
public class SimpleRelocator
    implements Relocator
{

    private final String pattern;

    private final String pathPattern;

    private final String shadedPattern;

    private final String shadedPathPattern;

    public SimpleRelocator( String patt, String shadedPattern)
    {
        if ( patt == null )
        {
            this.pattern = "";
            this.pathPattern = "";
        }
        else
        {
            this.pattern = patt.replace( '/', '.' );
            this.pathPattern = patt.replace( '.', '/' );
        }

        if ( shadedPattern != null )
        {
            this.shadedPattern = shadedPattern.replace( '/', '.' );
            this.shadedPathPattern = shadedPattern.replace( '.', '/' );
        }
        else
        {
            this.shadedPattern = "hidden." + this.pattern;
            this.shadedPathPattern = "hidden/" + this.pathPattern;
        }
    }

    public boolean canRelocatePath( String path )
    {
        if ( path.endsWith( ".class" ) )
        {
            path = path.substring( 0, path.length() - 6 );
        }

        // Allow for annoying option of an extra / on the front of a path. See MSHADE-119; comes from
        // getClass().getResource("/a/b/c.properties").
        return path.startsWith( pathPattern ) || path.startsWith ( "/" + pathPattern );
    }

    public boolean canRelocateClass( String clazz )
    {
        return clazz.indexOf( '/' ) < 0 && canRelocatePath( clazz.replace( '.', '/' ) );
    }

    public String relocatePath( String path )
    {
        return path.replaceFirst( pathPattern, shadedPathPattern );
    }

    public String relocateClass( String clazz )
    {
        return clazz.replaceFirst( pattern, shadedPattern );
    }

    public String applyToSourceContent( String sourceContent )
    {
        return sourceContent.replaceAll( "\\b" + pattern, shadedPattern );
    }
}
