package backtype.storm.codedistributor;

import backtype.storm.Config;
import com.google.common.collect.Lists;
import com.google.common.primitives.Shorts;
import com.turn.ttorrent.client.Client;
import com.turn.ttorrent.client.SharedTorrent;
import com.turn.ttorrent.common.Torrent;
import com.turn.ttorrent.tracker.TrackedTorrent;
import com.turn.ttorrent.tracker.Tracker;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.*;

public class BitTorrentCodeDistributor implements ICodeDistributor {
    private static final Logger LOG = LoggerFactory.getLogger(BitTorrentCodeDistributor.class);
    private Tracker tracker;
    private String hostName;
    private InetSocketAddress address;
    private Integer port;
    protected HashMap<String, Client> clients = new HashMap<String, Client>();
    protected Double maxDownload;
    protected Double maxUpload;
    private Integer seedDuration;

    @Override
    public void prepare(Map conf) throws Exception {
        this.hostName = InetAddress.getLocalHost().getCanonicalHostName();
        this.port = (Integer) conf.get(Config.BITTORRENT_PORT);
        this.maxDownload = (Double) conf.get(Config.BITTORRENT_MAX_DOWNLOAD_RATE);
        this.maxUpload = (Double) conf.get(Config.BITTORRENT_MAX_UPLOAD_RATE);
        this.seedDuration = (Integer) conf.get(Config.SUPERVISOR_BITTORRENT_SEED_DURATION);

        LOG.info("Download rates [U/D]: {}/{} kB/sec", format(this.maxUpload), format(this.maxDownload));

        LOG.info("Starting bt tracker bound to hostname '{}'", hostName);
        //using "0.0.0.0" to ensure we bind to all IPV4 network interfaces.
        this.address = new InetSocketAddress("0.0.0.0", port);

        this.tracker = new Tracker(address);
        LOG.info("Announce URL: {}", this.tracker.getAnnounceUrl());
        this.tracker.start();
    }

    @Override
    public File upload(String dirPath, String topologyId) throws Exception {
        File destDir = new File(dirPath);
        LOG.info("Generating torrent for directory: {}", destDir.getAbsolutePath());

        URI uri = URI.create("http://" + this.hostName + ":" + this.port + "/announce");
        LOG.info("Creating torrent with announce URL: {}", uri);

        //TODO: why does listing the directory not work?
        ArrayList<File> files = new ArrayList<File>();
        files.add(new File(destDir, "stormjar.jar"));
        files.add(new File(destDir, "stormconf.ser"));
        files.add(new File(destDir, "stormcode.ser"));

        Torrent torrent = Torrent.create(destDir, files, uri, "storm-nimbus");
        File torrentFile = new File(destDir, "storm-code-distributor.meta");
        torrent.save(new FileOutputStream(torrentFile));
        LOG.info("Saved torrent: {}", torrentFile.getAbsolutePath());
        this.tracker.announce(new TrackedTorrent(torrent));

        Client client = new Client(getInetAddress(), new SharedTorrent(torrent, destDir.getParentFile(), true));
        this.clients.put(topologyId, client);
        rebalanceRates();
        client.share();
        LOG.info("Seeding torrent...");

        /**
         *
         * TODO: Every time on prepare we need to call tracker.announce for all torrents that
         * exists in the file system, other wise the tracker will reject any peer request
         * with unknown torrents. You need to bootstrap trackers.
         */
        return torrentFile;
    }

    @Override
    public List<File> download(String topologyId, File torrentFile) throws Exception {
        LOG.info("Initiating BitTorrent download.");

        File destDir = torrentFile.getParentFile();
        LOG.info("Downloading with torrent file: {}", torrentFile.getAbsolutePath());
        LOG.info("Saving files to directory: {}", destDir.getAbsolutePath());
        SharedTorrent st = SharedTorrent.fromFile(torrentFile, destDir);
        Client client = new Client(getInetAddress(), st);
        this.clients.put(topologyId, client);
        rebalanceRates();
        client.share(this.seedDuration);

        //TODO: Should have a timeout after which we just fail the supervisor.
        if (this.seedDuration == 0) {
            client.waitForCompletion();
        } else {
            LOG.info("Waiting for seeding to begin...");
            while (client.getState() != Client.ClientState.SEEDING && client.getState() != Client.ClientState.ERROR) {
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

        File srcDir = new File(destDir, topologyId);
        for (File file : srcDir.listFiles()) {
            FileUtils.copyFileToDirectory(file, destDir);
            file.delete();
        }
        srcDir.delete();

        return Lists.newArrayList(destDir.listFiles());
    }

    private InetAddress getInetAddress() throws UnknownHostException {
        for (InetAddress addr : InetAddress.getAllByName(this.hostName)) {
            if (!addr.isAnyLocalAddress() && !addr.isLoopbackAddress() && !addr.isMulticastAddress()) {
                return addr;
            }
        }

        throw new RuntimeException("No valid InetAddress could be obtained, something really wrong with network configuration.");
    }

    @Override
    public short getReplicationCount(String topologyId) {
        Collection<TrackedTorrent> trackedTorrents = tracker.getTrackedTorrents();
        for (final TrackedTorrent trackedTorrent : trackedTorrents) {
            if (trackedTorrent.getName().equals(topologyId)) {
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
        if (client != null) {
            Torrent torrent = client.getTorrent();
            client.stop();
            this.tracker.remove(torrent);
        }
        rebalanceRates();
    }

    @Override
    public void close(Map conf) {
        this.tracker.stop();
    }

    private synchronized void rebalanceRates() {
        int clientCount = this.clients.size();
        if (clientCount > 0) {
            double maxDl = this.maxDownload <= 0.0 ? this.maxDownload : this.maxDownload / clientCount;
            double maxUl = this.maxUpload <= 0.0 ? this.maxUpload : this.maxUpload / clientCount;
            LOG.info("Rebalancing bandwidth allocation based on {} topology torrents.", clientCount);
            LOG.info("Per-torrent allocation [D/U]: {}/{} kB/sec.", format(maxDl), format(maxUl));
            for (Client client : this.clients.values()) {
                client.setMaxDownloadRate(maxDl);
                client.setMaxUploadRate(maxUl);
            }
        }
    }

    private static String format(double val) {
        return val <= 0.0 ? "UNLIMITED" : String.format("%.2f", val);
    }
}
