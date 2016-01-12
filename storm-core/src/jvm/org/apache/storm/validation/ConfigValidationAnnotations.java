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

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import java.lang.annotation.ElementType;
import java.lang.annotation.RetentionPolicy;

/**
 * Note: every annotation interface must have method `validatorClass()`
 * For every annotation there must validator class to do the validation
 * To add another annotation for config validation, add another annotation @interface class.  Implement the corresponding
 * validator logic in a class in ConfigValidation.  Make sure validateField method in ConfigValidation knows how to use the validator
 * and which method definition/parameters to pass in based on what fields are in the annotation.  By default, params of annotations
 * will be passed into a constructor that takes a Map as a parameter.
 */
public class ConfigValidationAnnotations {
    /**
     * Field names for annotations
     */
    public static class ValidatorParams {
        static final String VALIDATOR_CLASS = "validatorClass";
        static final String TYPE = "type";
        static final String ENTRY_VALIDATOR_CLASSES = "entryValidatorClasses";
        static final String KEY_VALIDATOR_CLASSES = "keyValidatorClasses";
        static final String VALUE_VALIDATOR_CLASSES = "valueValidatorClasses";
        static final String KEY_TYPE = "keyType";
        static final String VALUE_TYPE = "valueType";
        static final String INCLUDE_ZERO = "includeZero";
        static final String ACCEPTED_VALUES = "acceptedValues";
        static final String IMPLEMENTS_CLASS = "implementsClass";
    }

    /**
     * Validators with fields: validatorClass and type
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface isType {
        Class validatorClass() default ConfigValidation.SimpleTypeValidator.class;

        Class type();
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface isStringList {
        Class validatorClass() default ConfigValidation.ListEntryTypeValidator.class;

        Class type() default String.class;
    }

    /**
     * validates each entry in a list is of a certain type
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface isListEntryType {
        Class validatorClass() default ConfigValidation.ListEntryTypeValidator.class;

        Class type();
    }

    /**
     * Validators with fields: validatorClass
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface isString {
        Class validatorClass() default ConfigValidation.StringValidator.class;
        String[] acceptedValues() default "";
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface isNumber {
        Class validatorClass() default ConfigValidation.NumberValidator.class;
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface isBoolean {
        Class validatorClass() default ConfigValidation.BooleanValidator.class;
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface isInteger {
        Class validatorClass() default ConfigValidation.IntegerValidator.class;
    }

    /**
     * Validates on object is not null
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface NotNull {
        Class validatorClass() default ConfigValidation.NotNullValidator.class;
    }

    /**
     * Validates that there are no duplicates in a list
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface isNoDuplicateInList {
        Class validatorClass() default ConfigValidation.NoDuplicateInListValidator.class;
    }

    /**
     * Validates each entry in a list with a list of validators
     * Validators with fields: validatorClass and entryValidatorClass
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface isListEntryCustom {
        Class validatorClass() default ConfigValidation.ListEntryCustomValidator.class;

        Class[] entryValidatorClasses();
    }

    /**
     * Validates the type of each key and value in a map
     * Validator with fields: validatorClass, keyValidatorClass, valueValidatorClass
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface isMapEntryType {
        Class validatorClass() default ConfigValidation.MapEntryTypeValidator.class;

        Class keyType();

        Class valueType();
    }

    /**
     * Validates a each key and value in a Map with a list of validators
     * Validator with fields: validatorClass, keyValidatorClasses, valueValidatorClasses
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface isMapEntryCustom {
        Class validatorClass() default ConfigValidation.MapEntryCustomValidator.class;

        Class[] keyValidatorClasses();

        Class[] valueValidatorClasses();
    }

    /**
     * Checks if a number is positive and whether zero inclusive
     * Validator with fields: validatorClass, includeZero
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface isPositiveNumber {
        Class validatorClass() default ConfigValidation.PositiveNumberValidator.class;

        boolean includeZero() default false;
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface isImplementationOfClass {
        Class validatorClass() default ConfigValidation.ImplementsClassValidator.class;

        Class implementsClass();
    }

    /**
     * Complex/custom type validators
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface isStringOrStringList {
        Class validatorClass() default ConfigValidation.StringOrStringListValidator.class;
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface isKryoReg {
        Class validatorClass() default ConfigValidation.KryoRegValidator.class;
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface isPowerOf2 {
        Class validatorClass() default ConfigValidation.PowerOf2Validator.class;
    }

    /**
     * For custom validators
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface CustomValidator {
        Class validatorClass();
    }
}

