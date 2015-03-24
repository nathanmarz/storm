package org.apache.storm.flux;

import org.junit.Test;

public class IntegrationTest {

    private static boolean skipTest = true;

    static {
        String skipStr = System.getProperty("skipIntegration");
        if(skipStr != null && skipStr.equalsIgnoreCase("false")){
            skipTest = false;
        }
    }



    @Test
    public void testRunTopologySource() throws Exception {
        if(!skipTest) {
            Flux.main(new String[]{"-s", "30000", "src/test/resources/configs/existing-topology.yaml"});
        }
    }
}
