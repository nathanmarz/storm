package backtype.storm.nimbus;


import java.io.File;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/**
 * Interface responsible to distribute code in the cluster.
 */
public interface ICodeDistributor {
    /**
     * Prepare this code distributor.
     * @param conf
     */
    void prepare(Map conf);

    /**
     * This API will perform the actual upload of the code to the distribution implementation.
     * The API should return a Meta file which should have enough information for downloader
     * so it can download the code e.g. for bittorrent it will be a torrent file, in case of something like HDFS or s3
     * it might have the actual directory where all the code is put.
     * @param dirPath directory where all the code to be distributed exists.
     * @param topologyId the topologyId for which the meta file needs to be created.
     * @return metaFile
     */
    File upload(Path dirPath, String topologyId);

    /**
     * Returns the file object with downloaded meta file.
     * @param topologyId
     * @return
     */
    File downloadMetaFile(String topologyId);


    /**
     * Given the topologyId and metafile, download the actual code and return the downloaded file's list.
     * @param topologyid
     * @param metafile
     * @return
     */
    List<File> download(String topologyid, File metafile);

    /**
     * Performs the cleanup.
     * @param topologyid
     */
    void cleanup(String topologyid);

    /**
     * Close this distributor.
     * @param conf
     */
    void close(Map conf);
}
