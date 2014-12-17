package backtype.storm.nimbus;

import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static backtype.storm.Config.*;
import static backtype.storm.utils.Utils.downloadFromHost;
import static backtype.storm.utils.Utils.newCurator;

/**
 * Created by pbrahmbhatt on 12/12/14.
 */
public class LocalFileSystemCodeDistributor implements ICodeDistributor {
    private static final Logger LOG = LoggerFactory.getLogger(LocalFileSystemCodeDistributor.class);
    private CuratorFramework zkClient;
    private Map conf;

    @Override
    public void prepare(Map conf) throws Exception {
        this.conf = conf;
        List<String> zkServers = (List<String>) conf.get(STORM_ZOOKEEPER_SERVERS);
        int port = (Integer) conf.get(STORM_ZOOKEEPER_PORT);
        zkClient = newCurator(conf, zkServers, port, (String) conf.get(STORM_ZOOKEEPER_ROOT));
        zkClient.start();
        //TODO secure zk.
    }

    @Override
    public File upload(String dirPath, String topologyId) throws Exception {
        ArrayList<File> files = new ArrayList<File>();
        File destDir = new File(dirPath);
        File[] localFiles = destDir.listFiles();

        List<String> filePaths = new ArrayList<String>(3);
        for (File file : localFiles) {
            filePaths.add(file.getAbsolutePath());
        }

        File metaFile = new File(destDir, "storm-code-distributor.meta");
        boolean isCreated = metaFile.createNewFile();
        if (isCreated) {
            FileUtils.writeLines(metaFile, filePaths);
        } else {
            LOG.warn("metafile " + metaFile.getAbsolutePath() + " already exists.");
        }

        LOG.info("Created meta file " + metaFile.getAbsolutePath() + " upload successful.");

        return metaFile;
    }

    @Override
    public List<File> download(String topologyid, File metafile) throws Exception {
        List<String> hostInfos = zkClient.getChildren().forPath("/code-distributor/" + topologyid);
        File destDir = metafile.getParentFile();
        List<File> downloadedFiles = Lists.newArrayList();
        for (String absoluteFilePath : FileUtils.readLines(metafile)) {

            File localFile = new File(destDir, new File(absoluteFilePath).getName());

            boolean isSuccess = false;
            for (String hostAndPort : hostInfos) {
                String host = hostAndPort.split(":")[0];
                int port = Integer.parseInt(hostAndPort.split(":")[1]);
                try {
                    downloadFromHost(conf, absoluteFilePath, localFile.getAbsolutePath(), host, port);
                    downloadedFiles.add(localFile);
                    isSuccess = true;
                    break;
                } catch (Exception e) {
                    LOG.error("download failed from {}:{}, will try another endpoint ", host, port, e);
                }
            }

            if(!isSuccess) {
                throw new RuntimeException("File " + absoluteFilePath +" could not be downloaded from any endpoint");
            }
        }

        return downloadedFiles;
    }

    @Override
    public short getReplicationCount(String topologyId) throws Exception {
        return (short) zkClient.getChildren().forPath("/code-distributor/" + topologyId).size();
    }

    @Override
    public void cleanup(String topologyid) throws IOException {
        //no op.
    }

    @Override
    public void close(Map conf) {
       zkClient.close();
    }
}
