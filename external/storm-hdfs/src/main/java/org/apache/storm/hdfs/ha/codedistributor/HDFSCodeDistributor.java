package org.apache.storm.hdfs.ha.codedistributor;

import backtype.storm.nimbus.ICodeDistributor;
import backtype.storm.torrent.NimbusTracker;
import com.google.common.collect.Lists;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.Validate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.List;
import java.util.Map;

import static java.lang.System.lineSeparator;

public class HDFSCodeDistributor implements ICodeDistributor {
    private static final Logger LOG = LoggerFactory.getLogger(NimbusTracker.class);

    private final static String HDFS_STORM_DIR = "hdfs.storm.dir";

    private FileSystem fs;
    private Path stormDir;

    @Override
    public void prepare(Map conf) throws Exception {
        //TODO need to handle secure HDFS, also this assumes the hdfs-site.xml, core-site.xml and yarn-site.xml is
        //part of the classPath , document the assumption in README.md

        Validate.notNull(conf.get("hdfs.storm.dir"), "you must specify hdfs.storm.dir");
        this.fs = FileSystem.get(new Configuration());
        this.stormDir = new Path(String.valueOf(conf.get("hdfs.storm.dir")));
        if(!this.fs.exists(stormDir)) {
            this.fs.mkdirs(this.stormDir);
        }
    }

    @Override
    public File upload(String dirPath, String topologyId) throws Exception {
        File localStormDir = new File(dirPath);
        LOG.info("Copying the storm code from directory: {} to {}{}{}", localStormDir.getAbsolutePath(),
                stormDir.toString(), lineSeparator().toString(), topologyId);

        File[] files = localStormDir.listFiles();

        Path hdfsDestPath = new Path(stormDir, new Path(topologyId)); //TODO this might be wrong, we want stormDir/topologyId as destination path
        fs.mkdirs(hdfsDestPath);

        for(File file : files) {
            fs.copyFromLocalFile(new Path(file.getAbsolutePath()), hdfsDestPath);
        }

        //TODO : should not be .torrent , its misleading, but right now the download meta file is still part of supervisor and nimbus.
        // Once we have downloadMetaFile as part of ICodeDistributor, this should change.
        File file = new File(dirPath, topologyId +".torrent");

        RemoteIterator<LocatedFileStatus> hdfsFileIterator = fs.listFiles(hdfsDestPath, false);

        BufferedWriter writer = new BufferedWriter(new FileWriter(file));
        while(hdfsFileIterator.hasNext()) {
            writer.write(hdfsFileIterator.next().getPath().toString());
            writer.newLine();
        }
        writer.close();

        return file;
    }

    @Override
    public List<File> download(String topologyId, File metaFile) throws Exception {
        File destDir = metaFile.getParentFile();

        List<String> hdfsPaths = IOUtils.readLines(new FileInputStream(metaFile));
        for(String hdfsFilePath : hdfsPaths) {
            fs.copyToLocalFile(new Path(hdfsFilePath), new Path(destDir.getAbsolutePath()));
        }

        return Lists.newArrayList(destDir.listFiles());
    }

    @Override
    public short getReplicationCount(String topologyId) throws IOException {
        //TODO: we might want to check if the code is actually loaded to hdfs and return hdfs replication factor
        // for the topologyId folder. That may require the interface to change and it will have to pass the
        // meta file input param here. For now assuming the caller only calls this if call to  upload succeeded.

        Path hdfsDestPath = new Path(stormDir, new Path(topologyId));
        if(fs.exists(hdfsDestPath)) {
            FileStatus fileStatus = fs.getFileStatus(hdfsDestPath);
            return fileStatus.getReplication();
        } else {
            LOG.warn("getReplicationCount called for {} but no such directory exists, returning 0", topologyId);
            return 0;
        }
    }

    @Override
    public void cleanup(String topologyId) throws IOException {
        Path hdfsDestPath = new Path(stormDir, new Path(topologyId));
        fs.delete(hdfsDestPath, true);
    }

    @Override
    public void close(Map conf) {
        //TODO Should we delete all the files?
    }
}
