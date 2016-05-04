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

package org.apache.storm.validation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * Provides functionality for validating configuration fields.
 */
public class ConfigValidation {

    private static final Class CONFIG_CLASS = org.apache.storm.Config.class;

    private static final Logger LOG = LoggerFactory.getLogger(ConfigValidation.class);

    public static abstract class Validator {
        public Validator(Map<String, Object> params) {}
        public Validator() {}
        public abstract void validateField(String name, Object o);
    }

    /*
     * Validator definitions
     */

    /**
     * Validates if an object is not null
     */

    public static class NotNullValidator extends Validator {

        @Override
        public void validateField(String name, Object o) {
            if (o == null) {
                throw new IllegalArgumentException("Field " + name + "cannot be null! Actual value: " + o);
            }
        }
    }

    /**
     * Validates basic types
     */
    public static class SimpleTypeValidator extends Validator {

        private Class type;

        public SimpleTypeValidator(Map<String, Object> params) {
            this.type = (Class) params.get(ConfigValidationAnnotations.ValidatorParams.TYPE);
        }

        @Override
        public void validateField(String name, Object o) {
            validateField(name, this.type, o);
        }

        public static void validateField(String name, Class type, Object o) {
            if (o == null) {
                return;
            }
            if (type.isInstance(o)) {
                return;
            }
            throw new IllegalArgumentException("Field " + name + " must be of type " + type + ". Object: " + o + " actual type: " + o.getClass());
        }
    }

    public static class StringValidator extends Validator {

        private HashSet<String> acceptedValues = null;

        public StringValidator(){}

        public StringValidator(Map<String, Object> params) {

            this.acceptedValues = new HashSet<String>(Arrays.asList((String[])params.get(ConfigValidationAnnotations.ValidatorParams.ACCEPTED_VALUES)));

            if(this.acceptedValues.isEmpty() || (this.acceptedValues.size() == 1 && this.acceptedValues.contains(""))) {
                this.acceptedValues = null;
            }
        }

        @Override
        public void validateField(String name, Object o) {
            SimpleTypeValidator.validateField(name, String.class, o);
            if(this.acceptedValues != null) {
                if (!this.acceptedValues.contains((String) o)) {
                    throw new IllegalArgumentException("Field " + name + " is not an accepted value. Value: " + o + " Accepted values: " + this.acceptedValues);
                }
            }
        }
    }

    public static class BooleanValidator extends Validator {

        @Override
        public void validateField(String name, Object o) {
            SimpleTypeValidator.validateField(name, Boolean.class, o);
        }
    }

    public static class NumberValidator extends Validator {

        @Override
        public void validateField(String name, Object o) {
            SimpleTypeValidator.validateField(name, Number.class, o);
        }
    }

    public static class DoubleValidator extends Validator {

        @Override
        public void validateField(String name, Object o) {
            SimpleTypeValidator.validateField(name, Double.class, o);
        }
    }

    /**
     * Validates a Integer.
     */
    public static class IntegerValidator extends Validator {

        @Override
        public void validateField(String name, Object o) {
            validateInteger(name, o);
        }

        public void validateInteger(String name, Object o) {
            if (o == null) {
                return;
            }
            final long i;
            if (o instanceof Number &&
                    (i = ((Number) o).longValue()) == ((Number) o).doubleValue()) {
                if (i <= Integer.MAX_VALUE && i >= Integer.MIN_VALUE) {
                    return;
                }
            }
            throw new IllegalArgumentException("Field " + name + " must be an Integer within type range.");
        }
    }

    /**
     * Validates an entry for ImpersonationAclUser
     */
    public static class ImpersonationAclUserEntryValidator extends Validator {

        @Override
        public void validateField(String name, Object o) {
            if (o == null) {
                return;
            }
            ConfigValidationUtils.NestableFieldValidator validator = ConfigValidationUtils.mapFv(ConfigValidationUtils.fv(String.class, false),
                    ConfigValidationUtils.listFv(String.class, false), false);
            validator.validateField(name, o);
            Map<String, List<String>> mapObject = (Map<String, List<String>>) o;
            if (!mapObject.containsKey("hosts")) {
                throw new IllegalArgumentException(name + " should contain Map entry with key: hosts");
            }
            if (!mapObject.containsKey("groups")) {
                throw new IllegalArgumentException(name + " should contain Map entry with key: groups");
            }
        }
    }

    /**
     * validates a list of has no duplicates
     */
    public static class NoDuplicateInListValidator extends Validator {

        @Override
        public void validateField(String name, Object field) {
            if (field == null) {
                return;
            }
            //check if iterable
            SimpleTypeValidator.validateField(name, Iterable.class, field);
            HashSet<Object> objectSet = new HashSet<Object>();
            for (Object o : (Iterable) field) {
                if (objectSet.contains(o)) {
                    throw new IllegalArgumentException(name + " should contain no duplicate elements. Duplicated element: " + o);
                }
                objectSet.add(o);
            }
        }
    }

    /**
     * Validates a String or a list of Strings
     */
    public static class StringOrStringListValidator extends Validator {

        private ConfigValidationUtils.FieldValidator fv = ConfigValidationUtils.listFv(String.class, false);

        @Override
        public void validateField(String name, Object o) {

            if (o == null || o instanceof String) {
                // A null value or a String value is acceptable
                return;
            }
            this.fv.validateField(name, o);
        }
    }

    /**
     * Validates Kryo Registration
     */
    public static class KryoRegValidator extends Validator {

        @Override
        public void validateField(String name, Object o) {
            if (o == null) {
                return;
            }
            if (o instanceof Iterable) {
                for (Object e : (Iterable) o) {
                    if (e instanceof Map) {
                        for (Map.Entry<Object, Object> entry : ((Map<Object, Object>) e).entrySet()) {
                            if (!(entry.getKey() instanceof String) ||
                                    !(entry.getValue() instanceof String)) {
                                throw new IllegalArgumentException(
                                        "Each element of the list " + name + " must be a String or a Map of Strings");
                            }
                        }
                    } else if (!(e instanceof String)) {
                        throw new IllegalArgumentException(
                                "Each element of the list " + name + " must be a String or a Map of Strings");
                    }
                }
                return;
            }
            throw new IllegalArgumentException(
                    "Field " + name + " must be an Iterable containing only Strings or Maps of Strings");
        }
    }

    /**
     * Validates if a number is a power of 2
     */
    public static class PowerOf2Validator extends Validator {

        @Override
        public void validateField(String name, Object o) {
            if (o == null) {
                return;
            }
            final long i;
            if (o instanceof Number &&
                    (i = ((Number) o).longValue()) == ((Number) o).doubleValue()) {
                // Test whether the integer is a power of 2.
                if (i > 0 && (i & (i - 1)) == 0) {
                    return;
                }
            }
            throw new IllegalArgumentException("Field " + name + " must be a power of 2.");
        }
    }

    /**
     * Validates each entry in a list
     */
    public static class ListEntryTypeValidator extends Validator {

        private Class type;

        public ListEntryTypeValidator(Map<String, Object> params) {
            this.type = (Class) params.get(ConfigValidationAnnotations.ValidatorParams.TYPE);
        }

        @Override
        public void validateField(String name, Object o) {
            validateField(name, this.type, o);
        }

        public static void validateField(String name, Class type, Object o) {
            ConfigValidationUtils.NestableFieldValidator validator = ConfigValidationUtils.listFv(type, false);
            validator.validateField(name, o);
        }
    }

    /**
     * Validates each entry in a list against a list of custom Validators
     * Each validator in the list of validators must inherit or be an instance of Validator class
     */
    public static class ListEntryCustomValidator extends Validator{

        private Class[] entryValidators;

        public ListEntryCustomValidator(Map<String, Object> params) {
            this.entryValidators = (Class[]) params.get(ConfigValidationAnnotations.ValidatorParams.ENTRY_VALIDATOR_CLASSES);
        }

        @Override
        public void validateField(String name, Object o)  {
            try {
                validateField(name, this.entryValidators, o);
            } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException e) {
                throw new RuntimeException(e);
            }
        }

        public static void validateField(String name, Class[] validators, Object o) throws IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {
            if (o == null) {
                return;
            }
            //check if iterable
            SimpleTypeValidator.validateField(name, Iterable.class, o);
            for (Object entry : (Iterable) o) {
                for (Class validator : validators) {
                    Object v = validator.getConstructor().newInstance();
                    if (v instanceof Validator) {
                        ((Validator) v).validateField(name + " list entry", entry);
                    } else {
                        LOG.warn("validator: {} cannot be used in ListEntryCustomValidator.  Individual entry validators must a instance of Validator class", validator.getName());
                    }
                }
            }
        }
    }

    /**
     * validates each key and value in a map of a certain type
     */
    public static class MapEntryTypeValidator extends Validator{

        private Class keyType;
        private Class valueType;

        public MapEntryTypeValidator(Map<String, Object> params) {
            this.keyType = (Class) params.get(ConfigValidationAnnotations.ValidatorParams.KEY_TYPE);
            this.valueType = (Class) params.get(ConfigValidationAnnotations.ValidatorParams.VALUE_TYPE);
        }

        @Override
        public void validateField(String name, Object o) {
            validateField(name, this.keyType, this.valueType, o);
        }

        public static void validateField(String name, Class keyType, Class valueType, Object o) {
            ConfigValidationUtils.NestableFieldValidator validator = ConfigValidationUtils.mapFv(keyType, valueType, false);
            validator.validateField(name, o);
        }
    }

    /**
     * validates each key and each value against the respective arrays of validators
     */
    public static class MapEntryCustomValidator extends Validator{

        private Class[] keyValidators;
        private Class[] valueValidators;

        public MapEntryCustomValidator(Map<String, Object> params) {
            this.keyValidators = (Class []) params.get(ConfigValidationAnnotations.ValidatorParams.KEY_VALIDATOR_CLASSES);
            this.valueValidators = (Class []) params.get(ConfigValidationAnnotations.ValidatorParams.VALUE_VALIDATOR_CLASSES);
        }

        @Override
        public void validateField(String name, Object o) {
            try {
                validateField(name, this.keyValidators, this.valueValidators, o);
            } catch (IllegalAccessException | InstantiationException | NoSuchMethodException | InvocationTargetException e) {
                throw new RuntimeException(e);
            }
        }

        public static void validateField(String name, Class[] keyValidators, Class[] valueValidators, Object o) throws IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {
            if (o == null) {
                return;
            }
            //check if Map
            SimpleTypeValidator.validateField(name, Map.class, o);
            for (Map.Entry<Object, Object> entry : ((Map<Object, Object>) o).entrySet()) {
                for (Class kv : keyValidators) {
                    Object keyValidator = kv.getConstructor().newInstance();
                    if (keyValidator instanceof Validator) {
                        ((Validator) keyValidator).validateField(name + " Map key", entry.getKey());
                    } else {
                        LOG.warn("validator: {} cannot be used in MapEntryCustomValidator to validate keys.  Individual entry validators must a instance of Validator class", kv.getName());
                    }
                }
                for (Class vv : valueValidators) {
                    Object valueValidator = vv.getConstructor().newInstance();
                    if (valueValidator instanceof Validator) {
                        ((Validator) valueValidator).validateField(name + " Map value", entry.getValue());
                    } else {
                        LOG.warn("validator: {} cannot be used in MapEntryCustomValidator to validate values.  Individual entry validators must a instance of Validator class", vv.getName());
                    }
                }
            }
        }
    }

    /**
     * Validates a positive number
     */
    public static class PositiveNumberValidator extends Validator{

        private boolean includeZero;

        public PositiveNumberValidator() {
            this.includeZero = false;
        }

        public PositiveNumberValidator(Map<String, Object> params) {
            this.includeZero = (boolean) params.get(ConfigValidationAnnotations.ValidatorParams.INCLUDE_ZERO);
        }

        @Override
        public void validateField(String name, Object o) {
            validateField(name, this.includeZero, o);
        }

        public static void validateField(String name, boolean includeZero, Object o) {
            if (o == null) {
                return;
            }
            if (o instanceof Number) {
                if(includeZero) {
                    if (((Number) o).doubleValue() >= 0.0) {
                        return;
                    }
                } else {
                    if (((Number) o).doubleValue() > 0.0) {
                        return;
                    }
                }
            }
            throw new IllegalArgumentException("Field " + name + " must be a Positive Number");
        }
    }

    public static class MetricRegistryValidator extends Validator {

        @Override
        public void validateField(String name, Object o) {
            if(o == null) {
                return;
            }
            SimpleTypeValidator.validateField(name, Map.class, o);
            if(!((Map) o).containsKey("class") ) {
                throw new IllegalArgumentException( "Field " + name + " must have map entry with key: class");
            }
            if(!((Map) o).containsKey("parallelism.hint") ) {
                throw new IllegalArgumentException("Field " + name + " must have map entry with key: parallelism.hint");
            }

            SimpleTypeValidator.validateField(name, String.class, ((Map) o).get("class"));
            new IntegerValidator().validateField(name, ((Map) o).get("parallelism.hint"));
        }
    }

    public static class MapOfStringToMapOfStringToObjectValidator extends Validator {
      @Override
      public  void validateField(String name, Object o) {
        ConfigValidationUtils.NestableFieldValidator validator = ConfigValidationUtils.mapFv(ConfigValidationUtils.fv(String.class, false),
                ConfigValidationUtils.mapFv(String.class, Object.class,true), true);
        validator.validateField(name, o);
      }
    }

    public static class PacemakerAuthTypeValidator extends Validator {
        @Override
        public void validateField(String name, Object o) {
            if (o == null) {
                throw new IllegalArgumentException("Field " + name + " must be set.");
            }

            if (o instanceof String &&
                    (((String) o).equals("NONE") ||
                            ((String) o).equals("DIGEST") ||
                            ((String) o).equals("KERBEROS"))) {
                return;
            }
            throw new IllegalArgumentException("Field " + name + " must be one of \"NONE\", \"DIGEST\", or \"KERBEROS\"");
        }
    }

    public static class UserResourcePoolEntryValidator extends Validator {

        @Override
        public void validateField(String name, Object o) {
            if (o == null) {
                return;
            }
            SimpleTypeValidator.validateField(name, Map.class, o);
            if (!((Map) o).containsKey("cpu")) {
                throw new IllegalArgumentException("Field " + name + " must have map entry with key: cpu");
            }
            if (!((Map) o).containsKey("memory")) {
                throw new IllegalArgumentException("Field " + name + " must have map entry with key: memory");
            }

            SimpleTypeValidator.validateField(name, Number.class, ((Map) o).get("cpu"));
            SimpleTypeValidator.validateField(name, Number.class, ((Map) o).get("memory"));
        }
    }

    public static class ImplementsClassValidator extends Validator {

        Class classImplements;

        public ImplementsClassValidator(Map<String, Object> params) {
            this.classImplements = (Class) params.get(ConfigValidationAnnotations.ValidatorParams.IMPLEMENTS_CLASS);
        }

        @Override
        public void validateField(String name, Object o) {
            if (o == null) {
                return;
            }
            SimpleTypeValidator.validateField(name, String.class, o);
            try {
                Class objectClass = Class.forName((String) o);
                if (!this.classImplements.isAssignableFrom(objectClass)) {
                    throw new IllegalArgumentException("Field " + name + " with value " + o
                            + " does not implement " + this.classImplements.getName());
                }
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Methods for validating confs
     */

    /**
     * Validates a field given field name as string uses Config.java as the default config class
     *
     * @param fieldName provided as a string
     * @param conf      map of confs
     */
    public static void validateField(String fieldName, Map conf) {
        validateField(fieldName, conf, CONFIG_CLASS);
    }

    /**
     * Validates a field given field name as string
     *
     * @param fieldName   provided as a string
     * @param conf        map of confs
     * @param configClass config class
     */
    public static void validateField(String fieldName, Map conf, Class configClass) {
        Field field = null;
        try {
            field = configClass.getField(fieldName);
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
        validateField(field, conf);
    }

    /**
     * Validates a field given field.  Calls correct ValidatorField method based on which fields are
     * declared for the corresponding annotation.
     *
     * @param field field that needs to be validated
     * @param conf  map of confs
     */
    public static void validateField(Field field, Map conf) {
        Annotation[] annotations = field.getAnnotations();
        if (annotations.length == 0) {
            LOG.warn("Field {} does not have validator annotation", field);
        }
        try {
            for (Annotation annotation : annotations) {
                String type = annotation.annotationType().getName();
                Class validatorClass = null;
                Class<?>[] classes = ConfigValidationAnnotations.class.getDeclaredClasses();
                //check if annotation is one of our
                for (Class clazz : classes) {
                    if (clazz.getName().equals(type)) {
                        validatorClass = clazz;
                        break;
                    }
                }
                if (validatorClass != null) {
                    Object v = validatorClass.cast(annotation);
                    String key = (String) field.get(null);
                    Class clazz = (Class) validatorClass
                            .getMethod(ConfigValidationAnnotations.ValidatorParams.VALIDATOR_CLASS).invoke(v);
                    Validator o = null;
                    Map<String, Object> params = getParamsFromAnnotation(validatorClass, v);
                    //two constructor signatures used to initialize validators.
                    //One constructor takes input a Map of arguments, the other doesn't take any arguments (default constructor)
                    //If validator has a constructor that takes a Map as an argument call that constructor
                    if (hasConstructor(clazz, Map.class)) {
                        o = (Validator) clazz.getConstructor(Map.class).newInstance(params);
                    }
                    //If not call default constructor
                    else {
                        o = (((Class<Validator>) clazz).newInstance());
                    }
                    o.validateField(field.getName(), conf.get(key));
                }
            }
        }  catch (NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Validate all confs in map
     *
     * @param conf map of configs
     */
    public static void validateFields(Map conf) {
        validateFields(conf, CONFIG_CLASS);
    }

    /**
     * Validate all confs in map
     *
     * @param conf        map of configs
     * @param configClass config class
     */
    public static void validateFields(Map conf, Class configClass) {
        for (Field field : configClass.getFields()) {
            Object keyObj = null;
            try {
                keyObj = field.get(null);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
            //make sure that defined key is string in case wrong stuff got put into Config.java
            if (keyObj instanceof String) {
                String confKey = (String) keyObj;
                if (conf.containsKey(confKey)) {
                    validateField(field, conf);
                }
            }
        }
    }

    private static Map<String,Object> getParamsFromAnnotation(Class validatorClass, Object v) throws InvocationTargetException, IllegalAccessException {
        Map<String, Object> params = new HashMap<String, Object>();
        for(Method method : validatorClass.getDeclaredMethods()) {

            Object value = null;
            try {
                value = (Object) method.invoke(v);
            } catch (IllegalArgumentException ex) {
                value = null;
            }
            if(value != null) {
                params.put(method.getName(), value);
            }
        }
        return params;
    }

    private static boolean hasConstructor(Class clazz, Class paramClass) {
        Class[] classes = {paramClass};
        try {
            clazz.getConstructor(classes);
        } catch (NoSuchMethodException e) {
            return false;
        }
        return true;
    }

    private static boolean hasMethod(Class clazz, String method) {
        try {
            clazz.getMethod(method);
        } catch (NoSuchMethodException ex) {
            return false;
        }
        return true;
    }
}
