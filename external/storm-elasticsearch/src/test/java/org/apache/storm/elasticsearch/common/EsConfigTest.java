package org.apache.storm.elasticsearch.common;

import java.util.Map;
import java.util.UUID;

import com.google.common.collect.ImmutableMap;
import com.google.common.testing.NullPointerTester;

import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class EsConfigTest {

    private String clusterName = "name";
    private String[] nodes = new String[] {"localhost:9300"};

    @Test(expected = IllegalArgumentException.class)
    public void nodesCannotBeEmpty() throws Exception {
        new EsConfig(clusterName, new String[] {});
    }

    @Test
    public void settingsContainClusterName() throws Exception {
        EsConfig esConfig = new EsConfig(clusterName, nodes);
        assertThat(esConfig.toBasicSettings().get("cluster.name"), is(clusterName));
    }

    @Test
    public void usesAdditionalConfiguration() throws Exception {
        Map<String, String> additionalSettings = additionalSettings();
        EsConfig esConfig = new EsConfig(clusterName, nodes, additionalSettings);
        Settings settings = esConfig.toBasicSettings();
        assertSettingsContainAllAdditionalValues(settings, additionalSettings);
    }

    private Map<String, String> additionalSettings() {
        return ImmutableMap.of("client.transport.sniff", "true", UUID.randomUUID().toString(),
                               UUID.randomUUID().toString());
    }

    private void assertSettingsContainAllAdditionalValues(Settings settings, Map<String, String> additionalSettings) {
        for (String key : additionalSettings.keySet()) {
            assertThat(settings.get(key), is(additionalSettings.get(key)));
        }
    }

    @Test
    public void constructorThrowsOnNull() throws Exception {
        new NullPointerTester().testAllPublicConstructors(EsConfig.class);
    }
}
