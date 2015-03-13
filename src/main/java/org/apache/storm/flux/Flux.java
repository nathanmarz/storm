package org.apache.storm.flux;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.utils.Utils;
import org.apache.commons.cli.*;
import org.apache.storm.flux.model.ExecutionContext;
import org.apache.storm.flux.model.TopologyDef;
import org.apache.storm.flux.parser.FluxParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Flux entry point.
 *
 */
public class Flux {
    private static final Logger LOG = LoggerFactory.getLogger(Flux.class);

    private static final Long DEFAULT_LOCAL_SLEEP_TIME = 60000l;

    public static void main(String[] args) throws Exception {
        Options options = new Options();

        Option localOpt = OptionBuilder.hasArgs(0)
                .withArgName("local")
                .withLongOpt("local")
                .withDescription("Run the topology in local mode.")
                .create("l");
        options.addOption(localOpt);

        Option remoteOpt = OptionBuilder.hasArgs(0)
                .withArgName("remote")
                .withLongOpt("remote")
                .withDescription("Deploy the topology to a remote cluster.")
                .create("r");
        options.addOption(remoteOpt);

        Option resourceOpt = OptionBuilder.hasArgs(0)
                .withArgName("resource")
                .withLongOpt("resource")
                .withDescription("Treat the supplied path as a classpath resource instead of a file. (not implemented)")
                .create("R");
        options.addOption(resourceOpt);

        Option localSleepOpt = OptionBuilder.hasArgs(1)
                .withArgName("sleep")
                .withLongOpt("sleep")
                .withDescription("When running locally, the amount of time to sleep (in ms.) before killing the topology and shutting down the local cluster.")
                .create("s");
        options.addOption(localSleepOpt);


        CommandLineParser parser = new BasicParser();
        CommandLine cmd = parser.parse(options, args);

        if (cmd.getArgs().length != 1) {
            usage(options);
            System.exit(1);
        }
        runCli(cmd);
    }

    private static void usage(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("storm jar <my_topology_uber_jar.jar> " +
                Flux.class.getName() +
                " [options] <topology-config.yaml>", options);
    }

    private static void runCli(CommandLine cmd)throws Exception {
        TopologyDef topologyDef = FluxParser.parse((String)cmd.getArgList().get(0));


        String topologyName = topologyDef.getName();
        // merge contents of `config` into topology config
        Config conf = FluxBuilder.buildConfig(topologyDef);
        ExecutionContext context = new ExecutionContext(topologyDef, conf);
        StormTopology topology = FluxBuilder.buildTopology(context);



        if(cmd.hasOption("remote")) {
            LOG.info("Running remotely...");
            try {
                StormSubmitter.submitTopologyWithProgressBar(topologyName, conf, topology);
            } catch (Exception e){
                LOG.warn("Unable to deploy topology tp remote cluster.", e);
            }
        } else {
            LOG.info("Running in local mode...");
            LOG.debug("Sleep time: {}", cmd.getOptionValue("sleep"));
            String sleepStr =  cmd.getOptionValue("sleep");
            Long sleepTime = Long.parseLong(sleepStr);
            if(sleepTime == null){
                sleepTime = DEFAULT_LOCAL_SLEEP_TIME;
                LOG.debug("Using default sleep time of: " + sleepTime);

            }
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(topologyName, conf, topology);

            Utils.sleep(sleepTime);
            cluster.killTopology(topologyName);
            cluster.shutdown();
        }
    }
}
