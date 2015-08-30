package org.apache.storm.elasticsearch.trident;

import com.google.common.testing.NullPointerTester;

import org.apache.storm.elasticsearch.common.EsConfig;
import org.junit.Test;

public class EsStateFactoryTest {

    @Test
    public void constructorThrowsOnNull() throws Exception {
        new NullPointerTester().setDefault(EsConfig.class, new EsConfig("cluster", new String[] {"localhost:9300"}))
                               .testAllPublicConstructors(EsStateFactory.class);
    }
}
