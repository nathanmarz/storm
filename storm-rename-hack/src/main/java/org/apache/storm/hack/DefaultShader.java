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
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.commons.Remapper;
import org.objectweb.asm.commons.RemappingClassAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;
import java.util.jar.JarOutputStream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipException;

/**
 * This is based off of
 *
 * https://github.com/apache/maven-plugins.git
 *
 * maven-shade-plugin-2.4.1
 */
public class DefaultShader {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultShader.class);

    public void shadeJarStream(ShadeRequest shadeRequest, InputStream in, final OutputStream fileOutputStream)
            throws IOException {
        Set<String> resources = new HashSet<>();

        List<ResourceTransformer> transformers =
                new ArrayList<>( shadeRequest.getResourceTransformers() );
        LOG.debug("Transformers {}", transformers);

        RelocatorRemapper remapper = new RelocatorRemapper( shadeRequest.getRelocators() );
        LOG.debug("Remapper {}", remapper);

        JarOutputStream jos = new JarOutputStream( new BufferedOutputStream( fileOutputStream ) );

        JarInputStream jarFile = new JarInputStream(in);

        for ( JarEntry entry = jarFile.getNextJarEntry(); entry != null; entry = jarFile.getNextJarEntry())
        {
            String name = entry.getName();
            LOG.debug("Processing " + name);
            remapper.setClassName(name);
            if ( "META-INF/INDEX.LIST".equals( name ) )
            {
                LOG.debug("Skipping INDEX.LIST...");
                // we cannot allow the jar indexes to be copied over or the
                // jar is useless. Ideally, we could create a new one
                // later
                continue;
            }

            if ( !entry.isDirectory() )
            {
                InputStream is = jarFile;

                String mappedName = remapper.map( name );
                LOG.debug(name + " -> " + mappedName);

                int idx = mappedName.lastIndexOf( '/' );
                if ( idx != -1 )
                {
                    // make sure dirs are created
                    String dir = mappedName.substring( 0, idx );
                    if ( !resources.contains( dir ) )
                    {
                        addDirectory( resources, jos, dir );
                    }
                }

                if ( name.endsWith( ".class" ) )
                {
                    addRemappedClass( remapper, jos, name, is );
                }
                else if ( name.endsWith( ".java" ) )
                {
                    // Avoid duplicates
                    if ( resources.contains( mappedName ) )
                    {
                        continue;
                    }

                    addJavaSource( resources, jos, mappedName, is, shadeRequest.getRelocators() );
                }
                else
                {
                    if ( !resourceTransformed( transformers, mappedName, is, shadeRequest.getRelocators() ) )
                    {
                        // Avoid duplicates that aren't accounted for by the resource transformers
                        if ( resources.contains( mappedName ) )
                        {
                            continue;
                        }

                        addResource( resources, jos, mappedName, is );
                    }
                }
            }
        }

        jarFile.close();

        for ( ResourceTransformer transformer : transformers )
        {
            if ( transformer.hasTransformedResource() )
            {
                transformer.modifyOutputStream( jos );
            }
        }

        jos.close();
    }

    private void addDirectory( Set<String> resources, JarOutputStream jos, String name )
        throws IOException {
        if (name.lastIndexOf('/') > 0) {
            String parent = name.substring(0, name.lastIndexOf('/'));
            if (!resources.contains(parent)) {
                addDirectory(resources, jos, parent);
            }
        }

        // directory entries must end in "/"
        JarEntry entry = new JarEntry(name + "/");
        LOG.debug("Adding JAR directory " + entry);
        jos.putNextEntry(entry);

        resources.add(name);
    }

    private void addRemappedClass( RelocatorRemapper remapper, JarOutputStream jos, String name,
                                   InputStream is )
        throws IOException
    {
        LOG.debug("Remapping class... "+name);
        if ( !remapper.hasRelocators() )
        {
            try
            {
                LOG.debug("Just copy class...");
                jos.putNextEntry( new JarEntry( name ) );
                IOUtil.copy( is, jos );
            }
            catch ( ZipException e )
            {
                LOG.info( "zip exception ", e);
            }

            return;
        }

        ClassReader cr = new ClassReader( is );

        // We don't pass the ClassReader here. This forces the ClassWriter to rebuild the constant pool.
        // Copying the original constant pool should be avoided because it would keep references
        // to the original class names. This is not a problem at runtime (because these entries in the
        // constant pool are never used), but confuses some tools such as Felix' maven-bundle-plugin
        // that use the constant pool to determine the dependencies of a class.
        ClassWriter cw = new ClassWriter( 0 );

        final String pkg = name.substring( 0, name.lastIndexOf( '/' ) + 1 );
        ClassVisitor cv = new RemappingClassAdapter( cw, remapper )
        {
            @Override
            public void visitSource( final String source, final String debug )
            {
                LOG.debug("visitSource "+source);
                if ( source == null )
                {
                    super.visitSource( source, debug );
                }
                else
                {
                    final String fqSource = pkg + source;
                    final String mappedSource = remapper.map( fqSource );
                    final String filename = mappedSource.substring( mappedSource.lastIndexOf( '/' ) + 1 );
                    LOG.debug("Remapped to "+filename);
                    super.visitSource( filename, debug );
                }
            }
        };

        try
        {
            cr.accept( cv, ClassReader.EXPAND_FRAMES );
        }
        catch ( Throwable ise )
        {
            throw new IOException( "Error in ASM processing class " + name, ise );
        }

        byte[] renamedClass = cw.toByteArray();

        // Need to take the .class off for remapping evaluation
        String mappedName = remapper.map( name.substring( 0, name.indexOf( '.' ) ) );
        LOG.debug("Remapped class name to "+mappedName);

        try
        {
            // Now we put it back on so the class file is written out with the right extension.
            jos.putNextEntry( new JarEntry( mappedName + ".class" ) );
            jos.write(renamedClass);
        }
        catch ( ZipException e )
        {
            LOG.info( "zip exception ", e);
        }
    }

    private boolean resourceTransformed( List<ResourceTransformer> resourceTransformers, String name, InputStream is,
                                         List<Relocator> relocators )
        throws IOException
    {
        boolean resourceTransformed = false;

        for ( ResourceTransformer transformer : resourceTransformers )
        {
            if ( transformer.canTransformResource( name ) )
            {
                LOG.debug( "Transforming " + name + " using " + transformer.getClass().getName() );

                transformer.processResource( name, is, relocators );

                resourceTransformed = true;

                break;
            }
        }
        return resourceTransformed;
    }

    private void addJavaSource( Set<String> resources, JarOutputStream jos, String name, InputStream is,
                                List<Relocator> relocators )
        throws IOException
    {
        jos.putNextEntry( new JarEntry( name ) );

        String sourceContent = IOUtil.toString( new InputStreamReader( is, "UTF-8" ) );

        for ( Relocator relocator : relocators )
        {
            sourceContent = relocator.applyToSourceContent( sourceContent );
        }

        OutputStreamWriter writer = new OutputStreamWriter( jos, "UTF-8" );
        writer.append(sourceContent);
        writer.flush();

        resources.add( name );
    }

    private void addResource( Set<String> resources, JarOutputStream jos, String name, InputStream is )
        throws IOException
    {
        jos.putNextEntry( new JarEntry( name ) );

        IOUtil.copy( is, jos );

        resources.add( name );
    }

    class RelocatorRemapper extends Remapper
    {

        private final Pattern classPattern = Pattern.compile( "(\\[*)?L(.+);" );

        private final List<Relocator> relocators;

        private final HashSet<String> warned = new HashSet<>();

        private String className = "UNKNOWN";

        public RelocatorRemapper( List<Relocator> relocators)
        {
            this.relocators = relocators;
        }

        public boolean hasRelocators()
        {
            return !relocators.isEmpty();
        }

        public void setClassName(String className) {
            this.className = className;
        }

        @Override
        public Object mapValue( Object object )
        {
            if ( object instanceof String )
            {
                String name = (String) object;
                String value = name;

                String prefix = "";
                String suffix = "";

                Matcher m = classPattern.matcher( name );
                if ( m.matches() )
                {
                    prefix = m.group( 1 ) + "L";
                    suffix = ";";
                    name = m.group( 2 );
                }

                for ( Relocator r : relocators )
                {
                    if ( r.canRelocateClass( name ) )
                    {
                        value = prefix + r.relocateClass( name ) + suffix;
                        break;
                    }
                    else if ( r.canRelocatePath( name ) )
                    {
                        value = prefix + r.relocatePath( name ) + suffix;
                        break;
                    }
                }

                return value;
            }

            return super.mapValue( object );
        }

        @Override
        public String map( String name )
        {
            String orig = name;
            String value = name;

            String prefix = "";
            String suffix = "";

            Matcher m = classPattern.matcher( name );
            if ( m.matches() )
            {
                prefix = m.group( 1 ) + "L";
                suffix = ";";
                name = m.group( 2 );
            }

            for ( Relocator r : relocators )
            {
                if ( r.canRelocatePath( name ) )
                {
                    value = prefix + r.relocatePath( name ) + suffix;
                    if (!warned.contains(orig)) {
                        LOG.warn("Relocating {} to {} in {} please modify your code to use the new namespace", orig, value, className);
                        warned.add(orig);
                    }
                    break;
                }
            }

            return value;
        }

    }

}
