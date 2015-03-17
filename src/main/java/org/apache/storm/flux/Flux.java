package org.apache.storm.flux;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.utils.Utils;
import org.apache.commons.cli.*;
import org.apache.storm.flux.model.*;
import org.apache.storm.flux.parser.FluxParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

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
                .withDescription("Treat the supplied path as a classpath resource instead of a file.")
                .create("R");
        options.addOption(resourceOpt);

        Option localSleepOpt = OptionBuilder.hasArgs(1)
                .withArgName("sleep")
                .withLongOpt("sleep")
                .withDescription("When running locally, the amount of time to sleep (in ms.) before killing the " +
                        "topology and shutting down the local cluster.")
                .create("s");
        options.addOption(localSleepOpt);

        Option dryRunOpt = OptionBuilder.hasArgs(0)
                .withArgName("dry-run")
                .withLongOpt("dry-run")
                .withDescription("Do not run or deploy the topology. Just build, validate, and print information about " +
                        "the topology.")
                .create("d");
        options.addOption(dryRunOpt);

        Option noDetailOpt = OptionBuilder.hasArgs(0)
                .withArgName("no-detail")
                .withLongOpt("no-detail")
                .withDescription("Supress the printing of topology details.")
                .create("q");
        options.addOption(noDetailOpt);


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
        printSplash();


        boolean dumpYaml = cmd.hasOption("dump-yaml");


        TopologyDef topologyDef = null;
        String filePath = (String)cmd.getArgList().get(0);
        if(cmd.hasOption("resource")){
            printf("Parsing classpath resource: %s", filePath);
            topologyDef = FluxParser.parseResource(filePath, dumpYaml, true);
        } else {
            printf("Parsing classpath resource: %s",
                    new File(filePath).getAbsolutePath());
            topologyDef = FluxParser.parseFile(filePath, dumpYaml, true);
        }


        String topologyName = topologyDef.getName();
        // merge contents of `config` into topology config
        Config conf = FluxBuilder.buildConfig(topologyDef);
        ExecutionContext context = new ExecutionContext(topologyDef, conf);
        StormTopology topology = FluxBuilder.buildTopology(context);

        if(!cmd.hasOption("no-detail")){
            printTopologyInfo(context);
        }



        if(!cmd.hasOption("dry-run")) {
            if (cmd.hasOption("remote")) {
                LOG.info("Running remotely...");
                try {
                    StormSubmitter.submitTopologyWithProgressBar(topologyName, conf, topology);
                } catch (Exception e) {
                    LOG.warn("Unable to deploy topology tp remote cluster.", e);
                }
            } else {
                LOG.info("Running in local mode...");
                LOG.debug("Sleep time: {}", cmd.getOptionValue("sleep"));
                String sleepStr = cmd.getOptionValue("sleep");
                Long sleepTime = Long.parseLong(sleepStr);
                if (sleepTime == null) {
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

    static void printTopologyInfo(ExecutionContext ctx){
        TopologyDef t = ctx.getTopologyDef();
        print("---------- TOPOLOGY DETAILS ----------");

        printf("Name: %s", t.getName());
        print("--------------- SPOUTS ---------------");
        for(SpoutDef s : t.getSpouts()){
            printf("%s[%d](%s)", s.getId(), s.getParallelism(), s.getClassName());
        }
        print("---------------- BOLTS ---------------");
        for(BoltDef b : t.getBolts()){
            printf("%s[%d](%s)", b.getId(), b.getParallelism(), b.getClassName());
        }

        print("--------------- STREAMS ---------------");
        for(StreamDef sd : t.getStreams()){
            printf("%s --%s--> %s", sd.getFrom(), sd.getGrouping().getType(), sd.getTo());
        }
        print("--------------------------------------");
    }

    // save a little typing
    private static void printf(String format, Object... args){
        print(String.format(format, args));
    }

    private static void print(String string){
        System.out.println(string);
    }

    private static void printSplash() throws IOException {
        // banner
        InputStream is = Flux.class.getResourceAsStream("/splash.txt");
        if(is != null){
            BufferedReader br = new BufferedReader(new InputStreamReader(is));
            String line = null;
            while((line = br.readLine()) != null){
                System.out.println(line);
            }
        }
    }
}
