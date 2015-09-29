package org.apache.storm.hdfs.ha.codedistributor;

import backtype.storm.codedistributor.ICodeDistributor;
import com.google.common.collect.Lists;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.Validate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.storm.hdfs.common.security.HdfsSecurityUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.List;
import java.util.Map;

public class HDFSCodeDistributor implements ICodeDistributor {
    private static final Logger LOG = LoggerFactory.getLogger(HDFSCodeDistributor.class);

    private final static String HDFS_STORM_DIR = "hdfs.storm.dir";

    private FileSystem fs;
    private Path stormDir;

    @Override
    public void prepare(Map conf) throws Exception {
        Validate.notNull(conf.get(HDFS_STORM_DIR), "you must specify " + HDFS_STORM_DIR);

        Configuration configuration = new Configuration();
        HdfsSecurityUtil.login(conf, configuration);
        this.fs = FileSystem.get(configuration);
        this.stormDir = new Path(String.valueOf(conf.get(HDFS_STORM_DIR)));
        if(!this.fs.exists(stormDir)) {
            this.fs.mkdirs(this.stormDir);
        }
    }

    @Override
    public File upload(String dirPath, String topologyId) throws Exception {
        File localStormDir = new File(dirPath);
        LOG.info("Copying the storm code from directory: {} to {}{}{}", localStormDir.getAbsolutePath(),
                stormDir.toString(), Path.SEPARATOR , topologyId);

        File[] files = localStormDir.listFiles();

        Path hdfsDestPath = new Path(stormDir, new Path(topologyId));
        fs.mkdirs(hdfsDestPath);

        for(File file : files) {
            fs.copyFromLocalFile(new Path(file.getAbsolutePath()), hdfsDestPath);
        }

        File file = new File(dirPath, "storm-code-distributor.meta");

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
    }
}
