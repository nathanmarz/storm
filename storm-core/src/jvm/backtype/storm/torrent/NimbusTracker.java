package backtype.storm.torrent;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;

import com.turn.ttorrent.client.Client;
import com.turn.ttorrent.client.SharedTorrent;
import com.turn.ttorrent.common.Torrent;
import com.turn.ttorrent.tracker.TrackedTorrent;
import com.turn.ttorrent.tracker.Tracker;

public class NimbusTracker {
    private static final Logger LOG = LoggerFactory.getLogger(NimbusTracker.class);
    private Tracker tracker;
    private InetAddress nimbusHost;
    private HashMap<String, Client> clients = new HashMap<String, Client>();
    private String hostName;
    private Integer port;
    
    public void stop(String topologyId){
        LOG.info("Stop seeding/tracking for topology {}", topologyId);
        Client client = this.clients.remove(topologyId);
        if(client != null){
            Torrent torrent = client.getTorrent();
            client.stop();
            this.tracker.remove(torrent);
        }
    }
    
    public void trackAndSeed(String fileName, String topologyId) throws IOException, NoSuchAlgorithmException, InterruptedException, URISyntaxException{
        File jarFile = new File(fileName);
        File destDir = jarFile.getParentFile();
        LOG.info("Generating torrent for file: {}", jarFile.getAbsolutePath());
        
        URI uri = URI.create("http://" + this.hostName + ":" + this.port + "/announce");
        LOG.info("Creating torrent with annound URL: {}", uri);
        Torrent torrent = Torrent.create(jarFile, uri, "storm-nimbus");
        File torrentFile = new File(destDir, "stormjar.torrent");
        torrent.save(new FileOutputStream(torrentFile));
        LOG.info("Saved torrent: {}" + torrentFile.getAbsolutePath());
        this.tracker.announce(new TrackedTorrent(torrent));
        LOG.info("Torrent announced to tracker.");
        Client client = new Client(this.nimbusHost, new SharedTorrent(torrent, destDir, true));
        client.share();
        this.clients.put(topologyId, client);
        LOG.info("Seeding torrent...");
    }
    
    
    public NimbusTracker (Map conf) throws IOException{
        this.hostName = (String)conf.get(Config.NIMBUS_HOST);
        this.port = (Integer)conf.get(Config.NIMBUS_BITTORRENT_PORT);
        String bindAddress = (String)conf.get(Config.NIMBUS_BITTORRENT_BIND_ADDRESS);
        
        LOG.info("Starting bt tracker bound to interface '{}'", bindAddress);
        this.nimbusHost = InetAddress.getByName(this.hostName);
        InetSocketAddress socketAddr = new InetSocketAddress(bindAddress, port);
        this.tracker = new Tracker(socketAddr);
        LOG.info("Announce URL: {}", this.tracker.getAnnounceUrl());
        this.tracker.start();
    }

}
