package org.apache.storm.flux.parser;

import org.apache.storm.flux.model.BoltDef;
import org.apache.storm.flux.model.SpoutDef;
import org.apache.storm.flux.model.TopologyDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.TypeDescription;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

public class FluxParser {
    private static final Logger LOG = LoggerFactory.getLogger(FluxParser.class);

    private FluxParser(){}

    public static TopologyDef parseFile(String inputFile) throws IOException {
        Yaml yaml = yaml();

        FileInputStream in = new FileInputStream(inputFile);
        TopologyDef topology = (TopologyDef)yaml.load(in);

        in.close();
        LOG.debug("Configuration (interpreted): \n" + yaml.dump(topology));
        return topology;
    }

    public static TopologyDef parseResource(String resource) throws IOException {
        Yaml yaml = yaml();

        InputStream in = FluxParser.class.getResourceAsStream(resource);

        TopologyDef topology = (TopologyDef)yaml.load(in);

        in.close();
        LOG.debug("Configuration (interpreted): \n" + yaml.dump(topology));
        return topology;
    }

    private static Yaml yaml(){
        Constructor constructor = new Constructor(TopologyDef.class);

        TypeDescription topologyDescription = new TypeDescription(TopologyDef.class);
        topologyDescription.putListPropertyType("spouts", SpoutDef.class);
        topologyDescription.putListPropertyType("bolts", BoltDef.class);
        constructor.addTypeDescription(topologyDescription);

        Yaml  yaml = new Yaml(constructor);
        return yaml;
    }
}
