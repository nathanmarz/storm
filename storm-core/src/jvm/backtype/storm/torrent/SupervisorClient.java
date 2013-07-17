package backtype.storm.torrent;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.NoSuchAlgorithmException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.turn.ttorrent.client.Client;
import com.turn.ttorrent.client.SharedTorrent;

public class SupervisorClient {
    private static final Logger LOG = LoggerFactory.getLogger(SupervisorClient.class);
    private String torrentPath;

    public SupervisorClient(String torrentPath){
        this.torrentPath = torrentPath;
    }
    
    public void download() throws IOException, NoSuchAlgorithmException{
            LOG.info("Initiating BitTorrent download.");
            InetAddress netAddr = InetAddress.getLocalHost();
            File torrentFile = new File(this.torrentPath);
            File destDir = torrentFile.getParentFile();
            
            SharedTorrent st = SharedTorrent.fromFile(torrentFile, destDir);
            
            Client client = new Client(netAddr, st);
            client.download();
            client.waitForCompletion();
            client.stop();
            
            LOG.info("BitTorrent download complete.");
    }
    
    
}
