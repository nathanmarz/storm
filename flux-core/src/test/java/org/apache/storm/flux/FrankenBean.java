/*
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
package org.apache.storm.flux;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Test class that is a hybrid java bean -- it has both standard
 * java bean setters, as well as public instance variables.
 */
public class FrankenBean {
    private static final Logger LOG = LoggerFactory.getLogger(FrankenBean.class);

    public String publicString;
    public boolean publicBoolean;

    private String privateString;
    private boolean privateBoolean;

    public void setPrivateBoolean(boolean b){
        this.privateBoolean = b;
    }

    public void setPrivateString(String string){
        this.privateString = string;
    }

    static class Test {
        public Test(int i){
            System.out.println("Constructor: " + i);
        }
    }

    public static void main(String[] args) throws Exception{
        Integer i = new Integer(1);

        Class clazz = Test.class;

        Constructor ctor = clazz.getConstructor(new Class[]{int.class});
        ctor.newInstance(i);

        System.out.println("isNumber: " + Number.class.isAssignableFrom(i.getClass()));
    }

    public static void main2(String[] args) throws Exception {
        Class clazz = Class.forName("org.apache.storm.flux.FrankenBean");
        HashMap<String, Object> props = new HashMap<String, Object>();
        props.put("publicString", "foo");
        props.put("privateString", "bar");

        props.put("privateBoolean", true);
        props.put("publicBoolean", true);

        props.put("notgonnafindit", "foobar");

        // only support default constructors for now
        Object instance = clazz.newInstance();

        for(String key : props.keySet()){
            Method setter = findSetter(clazz, key, props.get(key));
            if(setter != null){
                // invoke setter
                setter.invoke(instance, new Object[]{props.get(key)});
            } else {
                // look for a public instance variable
                Field field = findPublicField(clazz, key, props.get(key));
                if(field != null) {
                    field.set(instance, props.get(key));
                }
            }
        }

        LOG.info("Bean: {}", instance);

    }


    public static Field findPublicField(Class clazz, String property, Object arg){
        Field field = null;
        try{
            field = clazz.getField(property);
        } catch (NoSuchFieldException e){
            LOG.warn("Could not find setter or public variable for property: " + property, e);
        }
        return field;
    }

    public static Method findSetter(Class clazz, String property, Object arg){
        String setterName = toSetterName(property);

//        ArrayList<Method> candidates = new ArrayList<Method>();
        Method retval = null;
        Method[] methods = clazz.getMethods();
        for(Method method : methods){
            if(setterName.equals(method.getName())) {
                LOG.info("Found setter method: " + method.getName());
                retval = method;
            }
        }
        return retval;
    }

    public static String toSetterName(String name){
        return "set" + name.substring(0,1).toUpperCase() + name.substring(1, name.length());
    }

    public String toString(){
        return String.format("publicString: %s, privateString: %s, " +
                "publicBoolean: %s, privateBoolean: %s",
                this.publicString,
                this.privateString,
                this.publicBoolean,
                this.privateBoolean);
    }

}
