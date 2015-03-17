package org.apache.storm.flux;

import org.junit.Test;
import static org.junit.Assert.*;

public class FluxBuilderTest {

    @Test
    public void testIsPrimitiveNumber() throws Exception {
        assertTrue(FluxBuilder.isPrimitiveNumber(int.class));
        assertFalse(FluxBuilder.isPrimitiveNumber(boolean.class));
        assertFalse(FluxBuilder.isPrimitiveNumber(String.class));
    }
}
