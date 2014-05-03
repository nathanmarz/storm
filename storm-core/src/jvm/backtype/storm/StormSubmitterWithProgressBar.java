package backtype.storm;

import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.SubmitOptions;
import org.apache.commons.lang.StringUtils;

import java.util.Map;

/**
 * Submit topology with a progress bar
 */

public class StormSubmitterWithProgressBar {
    /**
     * Submits a topology to run on the cluster with a progress bar. A topology runs forever or until
     * explicitly killed.
     *
     *
     * @param name the name of the storm.
     * @param stormConf the topology-specific configuration. See {@link Config}.
     * @param topology the processing to execute.
     * @throws AlreadyAliveException if a topology with this name is already running
     * @throws InvalidTopologyException if an invalid topology was submitted
     */

    public static void submitTopology(String name, Map stormConf, StormTopology topology) throws AlreadyAliveException, InvalidTopologyException {
        submitTopology(name, stormConf, topology, null);
    }

    /**
     * Submits a topology to run on the cluster with a progress bar. A topology runs forever or until
     * explicitly killed.
     *
     *
     * @param name the name of the storm.
     * @param stormConf the topology-specific configuration. See {@link Config}.
     * @param topology the processing to execute.
     * @param opts to manipulate the starting of the topology
     * @throws AlreadyAliveException if a topology with this name is already running
     * @throws InvalidTopologyException if an invalid topology was submitted
     */

    public static void submitTopology(String name, Map stormConf, StormTopology topology, SubmitOptions opts) throws AlreadyAliveException, InvalidTopologyException {
        // show a progress bar so we know we're not stuck (especially on slow connections)
        StormSubmitter.submitTopology(name, stormConf, topology, opts, new StormSubmitter.ProgressListener() {
            private long totalBytes;
            private String srcFile;
            private String targetFile;

            @Override
            public void onStart(String srcFile, String targetFile, long totalBytes) {
                this.srcFile = srcFile;
                this.targetFile = targetFile;
                this.totalBytes = totalBytes;
                System.out.printf("Start uploading file '%s' to '%s' (%d bytes)\n", srcFile, targetFile, totalBytes);
            }

            @Override
            public void onProgress(long bytesUploaded) {
                int length = 50;
                int p = (int)((length * bytesUploaded) / totalBytes);
                String progress = StringUtils.repeat("=", p);
                String todo = StringUtils.repeat(" ", length - p);

                System.out.printf("\r[%s%s] %d / %d", progress, todo, bytesUploaded, totalBytes);
            }

            @Override
            public void onCompleted() {
                System.out.printf("\nFile '%s' uploaded to '%s' (%d bytes)\n", srcFile, targetFile, totalBytes);
            }
        });
    }
}
