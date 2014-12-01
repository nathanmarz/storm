package backtype.storm.torrent;

import backtype.storm.Config;
import backtype.storm.nimbus.ICodeDistributor;
import com.google.common.collect.Lists;
import com.google.common.primitives.Shorts;
import com.turn.ttorrent.client.Client;
import com.turn.ttorrent.client.SharedTorrent;
import com.turn.ttorrent.common.Torrent;
import com.turn.ttorrent.tracker.TrackedTorrent;
import com.turn.ttorrent.tracker.Tracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class BitTorrentCodeDistributor implements ICodeDistributor {
    private static final Logger LOG = LoggerFactory.getLogger(BitTorrentCodeDistributor.class);
    private Tracker tracker;
    private String hostName;
    private InetAddress host;
    private Integer port;
    protected HashMap<String, Client> clients = new HashMap<String, Client>();
    protected Double maxDownload;
    protected Double maxUpload;
    private Integer seedDuration;

    @Override
    public void prepare(Map conf) throws Exception {
        this.hostName = InetAddress.getLocalHost().getCanonicalHostName();
        this.port = (Integer)conf.get(Config.BITTORRENT_PORT);
        this.host = InetAddress.getLocalHost();
        this.maxDownload = (Double)conf.get(Config.BITTORRENT_MAX_DOWNLOAD_RATE);
        this.maxUpload = (Double)conf.get(Config.BITTORRENT_MAX_UPLOAD_RATE);
        this.seedDuration = (Integer)conf.get(Config.SUPERVISOR_BITTORRENT_SEED_DURATION);

        LOG.info("Download rates [U/D]: {}/{} kB/sec", format(this.maxUpload), format(this.maxDownload));

        LOG.info("Starting bt tracker bound to hostname '{}'", hostName);
        InetSocketAddress socketAddr = new InetSocketAddress(hostName, port);
        this.tracker = new Tracker(socketAddr);
        LOG.info("Announce URL: {}", this.tracker.getAnnounceUrl());
        this.tracker.start();
    }

    @Override
    public File upload(String dirPath, String topologyId) throws Exception {
        File destDir = new File(dirPath);
        LOG.info("Generating torrent for directory: {}", destDir.getAbsolutePath());

        URI uri = URI.create("http://" + this.hostName + ":" + this.port + "/announce");
        LOG.info("Creating torrent with announce URL: {}", uri);
        ArrayList<File> files = new ArrayList<File>();
        files.add(new File(destDir, "stormjar.jar"));
        files.add(new File(destDir, "stormconf.ser"));
        files.add(new File(destDir, "stormcode.ser"));

        Torrent torrent = Torrent.create(destDir, files, uri, "storm-nimbus");
        File torrentFile = new File(destDir, "storm-code-distributor.meta");
        torrent.save(new FileOutputStream(torrentFile));
        LOG.info("Saved torrent: {}" + torrentFile.getAbsolutePath());
        this.tracker.announce(new TrackedTorrent(torrent));
        LOG.info("Torrent announced to tracker.");
        Client client = new Client(host, new SharedTorrent(torrent, destDir.getParentFile(), true));
        this.clients.put(topologyId, client);
        rebalanceRates();
        client.share();
        LOG.info("Seeding torrent...");

        return torrentFile;
    }

    @Override
    public List<File> download(String topologyId, File torrentFile) throws Exception {
        LOG.info("Initiating BitTorrent download.");
        InetAddress netAddr = InetAddress.getLocalHost();

        //TODO: This should be configured, the assumption that the files should be downloaded
        //in parent folder is probably not best one.
        File destDir = torrentFile.getParentFile();
        LOG.info("Downloading with torrent file: {}", torrentFile.getAbsolutePath());
        LOG.info("Saving files to directory: {}", destDir.getAbsolutePath());
        SharedTorrent st = SharedTorrent.fromFile(torrentFile, destDir);

        Client client = new Client(netAddr, st);
        this.clients.put(topologyId, client);
        rebalanceRates();
        client.share(this.seedDuration);

        //TODO: Should have a timeout after which we just fail the supervisor.
        if(this.seedDuration == 0) {
            client.waitForCompletion();
        } else {
            LOG.info("Waiting for seeding to begin...");
            while(client.getState() != Client.ClientState.SEEDING && client.getState() != Client.ClientState.ERROR){
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                }
            }
        }
        LOG.info("BitTorrent download complete.");

        /**
         * This should not be needed. currently the bittorrent library uses the torrent name (which is topologyId)
         * as the folder name and downloads all the files under that folder. so we need to either download
         * the torrent files under /storm-local/supervisor/stormdist or nimbus/stormdist/ to ensure stormdist becomes
         * the parent of all torrent files and the actual code will be downloaded under stormdist/topologyId/.
         * Ideally we should be able to specify that the downloaded files must be downloaded under
         * given folder only and no extra folder needs to be created.
         */

        File srcDir = Paths.get(destDir.getPath(), topologyId).toFile();
        for(File file : srcDir.listFiles()) {
            Files.copy(file.toPath(), destDir.toPath().resolve(file.getName()));
            file.delete();
        }
        srcDir.delete();

        return Lists.newArrayList(destDir.listFiles());
    }

    @Override
    public short getReplicationCount(String topologyId) {
        Collection<TrackedTorrent> trackedTorrents = tracker.getTrackedTorrents();
        for(TrackedTorrent trackedTorrent: trackedTorrents) {
            //TODO this needs to be actual name and not topologyId
            if(trackedTorrent.getName().equals(topologyId)) {
                return Shorts.checkedCast(trackedTorrent.seeders());
            }
        }
        LOG.warn("No torrent found in tracker for topologyId = " + topologyId);
        return 0;
    }

    @Override
    public void cleanup(String topologyId) {
        LOG.info("Stop seeding/tracking for topology {}", topologyId);
        Client client = this.clients.remove(topologyId);
        if(client != null){
            Torrent torrent = client.getTorrent();
            client.stop();
            this.tracker.remove(torrent);
        }
        rebalanceRates();

        //TODO: ensure supervisor and nimbus will blow the stormroot/topologyId folder completely.
    }

    @Override
    public void close(Map conf) {
        //TODO should we delete all .torrent files?
    }

    private synchronized void rebalanceRates(){
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

    private static String format(double val){
        return val <= 0.0 ? "UNLIMITED" : String.format("%.2f", val);
    }
}
