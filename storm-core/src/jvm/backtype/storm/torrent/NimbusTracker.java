package backtype.storm.torrent;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;

import com.turn.ttorrent.client.Client;
import com.turn.ttorrent.client.SharedTorrent;
import com.turn.ttorrent.common.Torrent;
import com.turn.ttorrent.tracker.TrackedTorrent;
import com.turn.ttorrent.tracker.Tracker;

public class NimbusTracker extends BasePeer {
    private static final Logger LOG = LoggerFactory.getLogger(NimbusTracker.class);
    private Tracker tracker;
    private InetAddress nimbusHost;
    private String hostName;
    private Integer port;
    
    public NimbusTracker (Map conf) throws IOException{
        this.hostName = (String)conf.get(Config.NIMBUS_HOST);
        this.port = (Integer)conf.get(Config.BITTORRENT_PORT);
        this.maxDownload = (Double)conf.get(Config.BITTORRENT_MAX_DOWNLOAD_RATE);
        this.maxUpload = (Double)conf.get(Config.BITTORRENT_MAX_UPLOAD_RATE);
        LOG.info("Download rates [U/D]: {}/{} kB/sec", format(this.maxDownload), format(this.maxDownload));
        
        String bindAddress = (String)conf.get(hostName);
        
        LOG.info("Starting bt tracker bound to interface '{}'", bindAddress);
        this.nimbusHost = InetAddress.getByName(this.hostName);
        InetSocketAddress socketAddr = new InetSocketAddress(bindAddress, port);
        this.tracker = new Tracker(socketAddr);
        LOG.info("Announce URL: {}", this.tracker.getAnnounceUrl());
        this.tracker.start();
    }
    
    public void stop(String topologyId){
        LOG.info("Stop seeding/tracking for topology {}", topologyId);
        Client client = this.clients.remove(topologyId);
        if(client != null){
            Torrent torrent = client.getTorrent();
            client.stop();
            this.tracker.remove(torrent);
        }
        rebalanceRates();
    }
    
    public void trackAndSeed(String dir, String topologyId) throws IOException, NoSuchAlgorithmException, InterruptedException, URISyntaxException{
        
        File destDir = new File(dir);
        LOG.info("Generating torrent for directory: {}", destDir.getAbsolutePath());
        
        URI uri = URI.create("http://" + this.hostName + ":" + this.port + "/announce");
        LOG.info("Creating torrent with announce URL: {}", uri);
        ArrayList<File> files = new ArrayList<File>();
        files.add(new File(destDir, "stormjar.jar"));
        files.add(new File(destDir, "stormconf.ser"));
        files.add(new File(destDir, "stormcode.ser"));
        
        Torrent torrent = Torrent.create(destDir, files, uri, "storm-nimbus");
        File torrentFile = new File(destDir, topologyId + ".torrent");
        torrent.save(new FileOutputStream(torrentFile));
        LOG.info("Saved torrent: {}" + torrentFile.getAbsolutePath());
        this.tracker.announce(new TrackedTorrent(torrent));
        LOG.info("Torrent announced to tracker.");
        Client client = new Client(this.nimbusHost, new SharedTorrent(torrent, destDir.getParentFile(), true));
        this.clients.put(topologyId, client);
        rebalanceRates();
        client.share();
        LOG.info("Seeding torrent...");
    }


}
