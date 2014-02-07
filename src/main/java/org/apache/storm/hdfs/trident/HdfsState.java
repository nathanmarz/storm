package org.apache.storm.hdfs.trident;

import backtype.storm.task.IMetricsContext;
import backtype.storm.topology.FailedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.storm.hdfs.common.rotation.RotationAction;
import org.apache.storm.hdfs.trident.format.FileNameFormat;
import org.apache.storm.hdfs.trident.format.RecordFormat;
import org.apache.storm.hdfs.trident.rotation.FileRotationPolicy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.TridentCollector;
import storm.trident.state.State;
import storm.trident.tuple.TridentTuple;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class HdfsState implements State {

    public static class Options implements Serializable {
        private RecordFormat format;
        private String fsUrl;
        private String path;
        private FileRotationPolicy rotationPolicy;
        private FileNameFormat fileNameFormat;
        private ArrayList<RotationAction> rotationActions = new ArrayList<RotationAction>();

        public Options(){}

        public Options withRecordFormat(RecordFormat format){
            this.format = format;
            return this;
        }

        public Options withRotationPolicy(FileRotationPolicy policy){
            this.rotationPolicy = policy;
            return this;
        }

        public Options withFileNameFormat(FileNameFormat format){
            this.fileNameFormat = format;
            return this;
        }

        public Options withFsUrl(String url){
            this.fsUrl = url;
            return this;
        }

        public Options withPath(String path){
            this.path = path;
            return this;
        }

        public Options addRotationAction(RotationAction action){
            this.rotationActions.add(action);
            return this;
        }

    }

    public static final Logger LOG = LoggerFactory.getLogger(HdfsState.class);

    private FileSystem fs;
    private FSDataOutputStream out;
    private int rotation = 0;
    private long offset = 0;

    private Options options;

    HdfsState(Options options){
        this.options = options;
    }

    void prepare(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions){
        LOG.info("Preparing HDFS Bolt...");
        if(this.options.format == null) throw new IllegalStateException("RecordFormat must be specified.");;
        if(this.options.rotationPolicy == null) throw new IllegalStateException("RotationPolicy must be specified.");


        if(this.options.fsUrl ==  null || this.options.path == null){
            throw new IllegalStateException("File system URL and base path must be specified.");
        }
        this.options.fileNameFormat.prepare(conf, partitionIndex, numPartitions);

        try{
            Configuration hdfsConfig = new Configuration();
            this.fs = FileSystem.get(URI.create(this.options.fsUrl), hdfsConfig);
            out = this.fs.create(new Path(this.options.path, this.options.fileNameFormat.getName(this.rotation, System.currentTimeMillis())));
        } catch (Exception e){
            throw new RuntimeException("Error preparing HdfsBolt: " + e.getMessage(), e);
        }
    }



    @Override
    public void beginCommit(Long txId) {
    }

    @Override
    public void commit(Long txId) {
        try {
            this.out.hsync();
        } catch (IOException e) {
            LOG.warn("Commit failed.", e);
        }
    }

    public void updateState(List<TridentTuple> tuples, TridentCollector tridentCollector){
        try{
            for(TridentTuple tuple : tuples){

                byte[] bytes = this.options.format.format(tuple);
                out.write(bytes);
                this.offset += bytes.length;

                if(this.options.rotationPolicy.mark(tuple, this.offset)){
                    rotateOutputFile();
                    this.options.rotationPolicy.reset();
                    this.offset = 0;
                }
            }
        } catch (IOException e){
            LOG.warn("Failing batch due to IOException.", e);
            throw new FailedException(e);
        }
    }

    private void rotateOutputFile() throws IOException {
        LOG.info("Rotating output file...");
        long start = System.currentTimeMillis();
        this.out.hsync();
        this.out.close();
        this.rotation++;
        Path path = new Path(
                this.options.path,
                this.options.fileNameFormat.getName(this.rotation, System.currentTimeMillis())
        );
        this.out = this.fs.create(path);

        for(RotationAction action : this.options.rotationActions){
            action.execute(this.fs, path);

        }
        long time = System.currentTimeMillis() - start;
        LOG.info("File rotation took {} ms.", time);
    }
}
