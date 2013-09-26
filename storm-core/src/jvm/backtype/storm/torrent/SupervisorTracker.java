package backtype.storm.torrent;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;

import com.turn.ttorrent.client.Client;
import com.turn.ttorrent.client.SharedTorrent;
import com.turn.ttorrent.client.Client.ClientState;
import com.turn.ttorrent.common.Torrent;
import com.turn.ttorrent.tracker.Tracker;

public class SupervisorTracker extends BaseTracker{
    private static final Logger LOG = LoggerFactory.getLogger(SupervisorTracker.class);
    
    private Integer seedDuration;
    
    public SupervisorTracker(Map conf){
        LOG.info("Creating supervisor bt tracker.");
        this.maxDownload = (Double)conf.get(Config.SUPERVISOR_BITTORRENT_MAX_DOWNLOAD_RATE);
        this.maxUpload = (Double)conf.get(Config.SUPERVISOR_BITTORRENT_MAX_UPLOAD_RATE);
        this.seedDuration = (Integer)conf.get(Config.SUPERVISOR_BITTORRENT_SEED_DURATION);
        LOG.info("Download rates [U/D]: {}/{} kB/sec", format(this.maxDownload), format(this.maxDownload));
    }
    
    public void stop(String topologyId){
        LOG.info("Stopping bt client for topology {}", topologyId);
        Client client = this.clients.remove(topologyId);
        if(client != null){
            client.stop();
        }
        rebalanceRates();
    }

    public void download(String torrentPath, String topologyId) throws IOException, NoSuchAlgorithmException{
        LOG.info("Initiating BitTorrent download.");
        InetAddress netAddr = InetAddress.getLocalHost();
        File torrentFile = new File(torrentPath);
        File destDir = torrentFile.getParentFile();
        LOG.info("Downloading with torrent file: {}", torrentFile.getAbsolutePath());
        LOG.info("Saving files to directory: {}", destDir.getAbsolutePath());
        SharedTorrent st = SharedTorrent.fromFile(torrentFile, destDir);
        
        Client client = new Client(netAddr, st);
        this.clients.put(topologyId, client);
        rebalanceRates();
        client.share(this.seedDuration);
        if(this.seedDuration == 0){
            client.waitForCompletion();
        } else {
            LOG.info("Waiting for seeding to begin...");
            while(client.getState() != ClientState.SEEDING && client.getState() != ClientState.ERROR){
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                }
            }
        }
        LOG.info("BitTorrent download complete.");
    }
    
}
