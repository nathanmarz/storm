package org.apache.storm.flux.parser;

import org.apache.storm.flux.model.BoltDef;
import org.apache.storm.flux.model.SpoutDef;
import org.apache.storm.flux.model.TopologyDef;
import org.yaml.snakeyaml.TypeDescription;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.FileInputStream;

public class FluxParser {

    public static void main(String[] args) throws Exception {

        TopologyDef topology = FluxParser.parse("src/test/resources/configs/tck.yaml");


    }


    public static TopologyDef parse(String inputFile) throws Exception {
        Constructor constructor = new Constructor(TopologyDef.class);

        TypeDescription topologyDescription = new TypeDescription(TopologyDef.class);
        topologyDescription.putListPropertyType("spouts", SpoutDef.class);
        topologyDescription.putListPropertyType("bolts", BoltDef.class);


        constructor.addTypeDescription(topologyDescription);
        Yaml  yaml = new Yaml(constructor);

        FileInputStream in = new FileInputStream(inputFile);

        TopologyDef topology = (TopologyDef)yaml.load(in);

        in.close();

        System.out.println(yaml.dump(topology));
        return topology;

    }
}
