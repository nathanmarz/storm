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

package backtype.storm.validation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.HashSet;
import java.util.Map;

/**
 * Provides functionality for validating configuration fields.
 */
public class ConfigValidation {

    private static final Class CONFIG_CLASS = backtype.storm.Config.class;

    private static final Logger LOG = LoggerFactory.getLogger(ConfigValidation.class);

    public static abstract class Validator {
        public abstract void validateField(String name, Object o);
    }

    public abstract static class TypeValidator {
        public abstract void validateField(String name, Class type, Object o);
    }

    /**
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
    public static class SimpleTypeValidator extends TypeValidator {

        public void validateField(String name, Class type, Object o) {
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

        @Override
        public void validateField(String name, Object o) {
            SimpleTypeValidator validator = new SimpleTypeValidator();
            validator.validateField(name, String.class, o);
        }
    }

    public static class BooleanValidator extends Validator {

        @Override
        public void validateField(String name, Object o) {
            SimpleTypeValidator validator = new SimpleTypeValidator();
            validator.validateField(name, Boolean.class, o);
        }
    }

    public static class NumberValidator extends Validator {

        @Override
        public void validateField(String name, Object o) {
            SimpleTypeValidator validator = new SimpleTypeValidator();
            validator.validateField(name, Number.class, o);
        }
    }

    public static class DoubleValidator extends Validator {

        @Override
        public void validateField(String name, Object o) {
            SimpleTypeValidator validator = new SimpleTypeValidator();
            validator.validateField(name, Double.class, o);
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
     * Validates a map of Strings to a map of Strings to a list.
     * {str -> {str -> [str,str]}
     */
    public static class ImpersonationAclValidator extends Validator {

        @Override
        public void validateField(String name, Object o) {
            if (o == null) {
                return;
            }
            ConfigValidationUtils.NestableFieldValidator validator = ConfigValidationUtils.mapFv(ConfigValidationUtils.fv(String.class, false),
                    ConfigValidationUtils.mapFv(ConfigValidationUtils.fv(String.class, false),
                            ConfigValidationUtils.listFv(String.class, false), false), true);
            validator.validateField(name, o);
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
            SimpleTypeValidator isIterable = new SimpleTypeValidator();
            isIterable.validateField(name, Iterable.class, field);
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

            if (o == null) {
                return;
            }
            if (o instanceof String) {
                return;
            }
            //check if iterable
            SimpleTypeValidator isIterable = new SimpleTypeValidator();
            try {
                isIterable.validateField(name, Iterable.class, o);
            } catch (Exception ex) {
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
    public static class ListEntryTypeValidator extends TypeValidator {

        @Override
        public void validateField(String name, Class type, Object o) {
            ConfigValidationUtils.NestableFieldValidator validator = ConfigValidationUtils.listFv(type, false);
            validator.validateField(name, o);
        }
    }

    /**
     * Validates each entry in a list against a list of custom Validators
     * Each validator in the list of validators must inherit or be an instance of Validator class
     */
    public static class ListEntryCustomValidator {

        public void validateField(String name, Class[] validators, Object o) throws IllegalAccessException, InstantiationException {
            if (o == null) {
                return;
            }
            //check if iterable
            SimpleTypeValidator isIterable = new SimpleTypeValidator();
            isIterable.validateField(name, Iterable.class, o);
            for (Object entry : (Iterable) o) {
                for (Class validator : validators) {
                    Object v = validator.newInstance();
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
    public static class MapEntryTypeValidator {

        public void validateField(String name, Class keyType, Class valueType, Object o) {
            ConfigValidationUtils.NestableFieldValidator validator = ConfigValidationUtils.mapFv(keyType, valueType, false);
            validator.validateField(name, o);
        }
    }

    /**
     * validates each key and each value against the respective arrays of validators
     */
    public static class MapEntryCustomValidator {

        public void validateField(String name, Class[] keyValidators, Class[] valueValidators, Object o) throws IllegalAccessException, InstantiationException {
            if (o == null) {
                return;
            }
            //check if Map
            SimpleTypeValidator isMap = new SimpleTypeValidator();
            isMap.validateField(name, Map.class, o);
            for (Map.Entry<Object, Object> entry : ((Map<Object, Object>) o).entrySet()) {
                for (Class kv : keyValidators) {
                    Object keyValidator = kv.newInstance();
                    if (keyValidator instanceof Validator) {
                        ((Validator) keyValidator).validateField(name + " Map key", entry.getKey());
                    } else {
                        LOG.warn("validator: {} cannot be used in MapEntryCustomValidator to validate keys.  Individual entry validators must a instance of Validator class", kv.getName());
                    }
                }
                for (Class vv : valueValidators) {
                    Object valueValidator = vv.newInstance();
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

        @Override
        public void validateField(String name, Object o) {
            validateField(name, false, o);
        }

        public void validateField(String name, boolean includeZero, Object o) {
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

    /**
     * Methods for validating confs
     */

    /**
     * Validates a field given field name as string uses Config.java as the default config class
     *
     * @param fieldName provided as a string
     * @param conf      map of confs
     */
    public static void validateField(String fieldName, Map conf) throws NoSuchFieldException, IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {
        validateField(fieldName, conf, CONFIG_CLASS);
    }

    /**
     * Validates a field given field name as string
     *
     * @param fieldName   provided as a string
     * @param conf        map of confs
     * @param configClass config class
     */
    public static void validateField(String fieldName, Map conf, Class configClass) throws NoSuchFieldException, IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {
        Field field = configClass.getField(fieldName);
        validateField(field, conf);
    }

    /**
     * Validates a field given field.  Calls correct ValidatorField method based on which fields are
     * declared for the corresponding annotation.
     *
     * @param field field that needs to be validated
     * @param conf  map of confs
     */
    public static void validateField(Field field, Map conf) throws NoSuchFieldException, IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {
        Annotation[] annotations = field.getAnnotations();
        if (annotations.length == 0) {
            LOG.warn("Field {} does not have validator annotation", field);
        }
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
                        .getMethod(ConfigValidationAnnotations.VALIDATOR_CLASS).invoke(v);
                //Determining which method from which class should be invoked based on what fields/parameters the annotation has
                if (hasMethod(validatorClass, ConfigValidationAnnotations.TYPE)) {

                    TypeValidator o = ((Class<TypeValidator>) clazz).newInstance();

                    Class objectType = (Class) validatorClass.getMethod(ConfigValidationAnnotations.TYPE).invoke(v);

                    o.validateField(field.getName(), objectType, conf.get(key));

                } else if (hasMethod(validatorClass, ConfigValidationAnnotations.ENTRY_VALIDATOR_CLASSES)) {

                    ListEntryCustomValidator o = ((Class<ListEntryCustomValidator>) clazz).newInstance();

                    Class[] entryValidators = (Class[]) validatorClass.getMethod(ConfigValidationAnnotations.ENTRY_VALIDATOR_CLASSES).invoke(v);

                    o.validateField(field.getName(), entryValidators, conf.get(key));

                } else if (hasMethod(validatorClass, ConfigValidationAnnotations.KEY_VALIDATOR_CLASSES)
                        && hasMethod(validatorClass, ConfigValidationAnnotations.VALUE_VALIDATOR_CLASSES)) {

                    MapEntryCustomValidator o = ((Class<MapEntryCustomValidator>) clazz).newInstance();

                    Class[] keyValidators = (Class[]) validatorClass.getMethod(ConfigValidationAnnotations.KEY_VALIDATOR_CLASSES).invoke(v);

                    Class[] valueValidators = (Class[]) validatorClass.getMethod(ConfigValidationAnnotations.VALUE_VALIDATOR_CLASSES).invoke(v);

                    o.validateField(field.getName(), keyValidators, valueValidators, conf.get(key));

                } else if (hasMethod(validatorClass, ConfigValidationAnnotations.KEY_TYPE)
                        && hasMethod(validatorClass, ConfigValidationAnnotations.VALUE_TYPE)) {

                    MapEntryTypeValidator o = ((Class<MapEntryTypeValidator>) clazz).newInstance();

                    Class keyType = (Class) validatorClass.getMethod(ConfigValidationAnnotations.KEY_TYPE).invoke(v);

                    Class valueType = (Class) validatorClass.getMethod(ConfigValidationAnnotations.VALUE_TYPE).invoke(v);

                    o.validateField(field.getName(), keyType, valueType, conf.get(key));

                } else if (hasMethod(validatorClass, ConfigValidationAnnotations.INCLUDE_ZERO)) {

                    PositiveNumberValidator o = ((Class<PositiveNumberValidator>) clazz).newInstance();

                    Boolean includeZero = (Boolean) validatorClass.getMethod(ConfigValidationAnnotations.INCLUDE_ZERO).invoke(v);

                    o.validateField(field.getName(), includeZero, conf.get(key));

                }
                //For annotations that does not have any additional fields. Call corresponding validateField method
                else {

                    ConfigValidation.Validator o = ((Class<ConfigValidation.Validator>) clazz).newInstance();

                    o.validateField(field.getName(), conf.get(key));
                }
            }
        }
    }

    /**
     * Validate all confs in map
     *
     * @param conf map of configs
     */
    public static void validateFields(Map conf) throws IllegalAccessException, InstantiationException, NoSuchFieldException, NoSuchMethodException, InvocationTargetException {
        validateFields(conf, CONFIG_CLASS);
    }

    /**
     * Validate all confs in map
     *
     * @param conf        map of configs
     * @param configClass config class
     */
    public static void validateFields(Map conf, Class configClass) throws IllegalAccessException, InstantiationException, NoSuchFieldException, NoSuchMethodException, InvocationTargetException {
        for (Field field : configClass.getFields()) {
            Object keyObj = field.get(null);
            //make sure that defined key is string in case wrong stuff got put into Config.java
            if (keyObj instanceof String) {
                String confKey = (String) keyObj;
                if (conf.containsKey(confKey)) {
                    validateField(field, conf);
                }
            }
        }
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
