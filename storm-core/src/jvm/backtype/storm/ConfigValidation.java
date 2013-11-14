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
}
