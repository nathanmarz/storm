package backtype.storm.torrent;

import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.turn.ttorrent.client.Client;

public abstract class BasePeer {
    private static final Logger LOG = LoggerFactory.getLogger(BasePeer.class);

    protected HashMap<String, Client> clients = new HashMap<String, Client>();
    protected Double maxDownload;
    protected Double maxUpload;
    
    protected synchronized void rebalanceRates(){
        int clientCount = this.clients.size();
        if(clientCount > 0){
            double maxDl = this.maxDownload <= 0.0 ? this.maxDownload : this.maxDownload / clientCount;
            double maxUl = this.maxUpload <= 0.0 ? this.maxUpload : this.maxUpload / clientCount;
            LOG.info("Rebalancing bandwidth allocation based on {} topology torrents.", clientCount);
            LOG.info("Per-torrent allocation [D/U]: {}/{} kB/sec.", format(maxDl), format(maxUl));
            for(Client client : this.clients.values()) {
                client.setMaxDownloadRate(maxDl);
                client.setMaxUploadRate(maxUl);
            }
        }
    }
    
    protected static String format(double val){
        return val <= 0.0 ? "UNLIMITED" : String.format("%.2f", val);
    }
}
