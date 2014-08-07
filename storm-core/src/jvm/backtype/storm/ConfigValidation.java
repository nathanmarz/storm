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
package backtype.storm;
import java.util.Map;

/**
 * Provides functionality for validating configuration fields.
 */
public class ConfigValidation {

    /**
     * Declares methods for validating configuration values.
     */
    public static interface FieldValidator {
        /**
         * Validates the given field.
         * @param name the name of the field.
         * @param field The field to be validated.
         * @throws IllegalArgumentException if the field fails validation.
         */
        public void validateField(String name, Object field) throws IllegalArgumentException;
    }

    /**
     * Returns a new FieldValidator for a List of the given Class.
     * @param cls the Class of elements composing the list
     * @return a FieldValidator for a list of the given class
     */
    static FieldValidator FieldListValidatorFactory(final Class cls) {
        return new FieldValidator() {
            @Override
            public void validateField(String name, Object field)
                    throws IllegalArgumentException {
                if (field == null) {
                    // A null value is acceptable.
                    return;
                }
                if (field instanceof Iterable) {
                    for (Object e : (Iterable)field) {
                        if (! cls.isInstance(e)) {
                            throw new IllegalArgumentException(
                                    "Each element of the list " + name + " must be a " +
                                    cls.getName() + ".");
                        }
                    }
                    return;
                }
                throw new IllegalArgumentException(
                        "Field " + name + " must be an Iterable of " + cls.getName());
            }
        };
    }

    /**
     * Validates a list of Numbers.
     */
    public static Object NumbersValidator = FieldListValidatorFactory(Number.class);

    /**
     * Validates is a list of Strings.
     */
    public static Object StringsValidator = FieldListValidatorFactory(String.class);

    /**
     * Validates is a list of Maps.
     */
    public static Object MapsValidator = FieldListValidatorFactory(Map.class);

    /**
     * Validates a Integer.
     */
    public static Object IntegerValidator = new FieldValidator() {
        @Override
        public void validateField(String name, Object o) throws IllegalArgumentException {
            if (o == null) {
                // A null value is acceptable.
                return;
            }
            final long i;
            if (o instanceof Number &&
                    (i = ((Number)o).longValue()) == ((Number)o).doubleValue()) {
                if (i <= Integer.MAX_VALUE && i >= Integer.MIN_VALUE) {
                    return;
                }
            }

            throw new IllegalArgumentException("Field " + name + " must be an Integer within type range.");
        }
    };

    /**
     * Validates is a list of Integers.
     */
    public static Object IntegersValidator = new FieldValidator() {
        @Override
        public void validateField(String name, Object field)
                throws IllegalArgumentException {
            if (field == null) {
                // A null value is acceptable.
                return;
            }
            if (field instanceof Iterable) {
                for (Object o : (Iterable)field) {
                    final long i;
                    if (o instanceof Number &&
                            ((i = ((Number)o).longValue()) == ((Number)o).doubleValue()) &&
                            (i <= Integer.MAX_VALUE && i >= Integer.MIN_VALUE)) {
                        // pass the test
                    } else {
                        throw new IllegalArgumentException(
                                "Each element of the list " + name + " must be an Integer within type range.");
                    }
                }
                return;
            }
        }
    };

    /**
     * Validates a Double.
     */
    public static Object DoubleValidator = new FieldValidator() {
        @Override
        public void validateField(String name, Object o) throws IllegalArgumentException {
            if (o == null) {
                // A null value is acceptable.
                return;
            }

            // we can provide a lenient way to convert int/long to double with losing some precision
            if (o instanceof Number) {
                return;
            }

            throw new IllegalArgumentException("Field " + name + " must be an Double.");
        }
    };

    /**
     * Validates a power of 2.
     */
    public static Object PowerOf2Validator = new FieldValidator() {
        @Override
        public void validateField(String name, Object o) throws IllegalArgumentException {
            if (o == null) {
                // A null value is acceptable.
                return;
            }
            final long i;
            if (o instanceof Number &&
                    (i = ((Number)o).longValue()) == ((Number)o).doubleValue())
            {
                // Test whether the integer is a power of 2.
                if (i > 0 && (i & (i-1)) == 0) {
                    return;
                }
            }
            throw new IllegalArgumentException("Field " + name + " must be a power of 2.");
        }
    };

    /**
     * Validates Kryo Registration
     */
    public static Object KryoRegValidator = new FieldValidator() {
        @Override
        public void validateField(String name, Object o) throws IllegalArgumentException {
            if (o == null) {
                // A null value is acceptable.
                return;
            }
            if (o instanceof Iterable) {
                for (Object e : (Iterable)o) {
                    if (e instanceof Map) {
                        for (Map.Entry<Object,Object> entry: ((Map<Object,Object>)e).entrySet()) {
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
    };

    /**
     * Validates a String or a list of Strings
     */
    public static Object StringOrStringListValidator = new FieldValidator() {

        private FieldValidator fv = FieldListValidatorFactory(String.class);

        @Override
        public void validateField(String name, Object o) throws IllegalArgumentException {
            if (o == null || o instanceof String) {
                // A null value or a String value is acceptable
                return;
            }
            this.fv.validateField(name, o);
        }
    };
}
