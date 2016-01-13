/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.localizer;

import org.apache.storm.Config;
import org.apache.storm.blobstore.ClientBlobStore;
import org.apache.storm.blobstore.InputStreamWithMeta;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.KeyNotFoundException;
import org.apache.storm.utils.ShellUtils.ExitCodeException;
import org.apache.storm.utils.ShellUtils.ShellCommandExecutor;
import org.apache.storm.utils.Utils;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;

/**
 * Class to download and manage files from the blobstore.  It uses an LRU cache
 * to determine which files to keep so they can be reused and which files to delete.
 */
public class Localizer {
  public static final Logger LOG = LoggerFactory.getLogger(Localizer.class);

  private Map _conf;
  private int _threadPoolSize;
  // thread pool for initial download
  private ExecutorService _execService;
  // thread pool for updates
  private ExecutorService _updateExecService;
  private int _blobDownloadRetries;

  // track resources - user to resourceSet
  private final ConcurrentMap<String, LocalizedResourceSet> _userRsrc = new
      ConcurrentHashMap<String, LocalizedResourceSet>();

  private String _localBaseDir;
  public static final String USERCACHE = "usercache";
  public static final String FILECACHE = "filecache";

  // sub directories to store either files or uncompressed archives respectively
  public static final String FILESDIR = "files";
  public static final String ARCHIVESDIR = "archives";

  private static final String TO_UNCOMPRESS = "_tmp_";

  // cleanup
  private long _cacheTargetSize;
  private long _cacheCleanupPeriod;
  private ScheduledExecutorService _cacheCleanupService;

  public Localizer(Map conf, String baseDir) {
    _conf = conf;
    _localBaseDir = baseDir;
    // default cache size 10GB, converted to Bytes
    _cacheTargetSize = Utils.getInt(_conf.get(Config.SUPERVISOR_LOCALIZER_CACHE_TARGET_SIZE_MB),
            10 * 1024).longValue() << 20;
    // default 10 minutes.
    _cacheCleanupPeriod = Utils.getInt(_conf.get(
            Config.SUPERVISOR_LOCALIZER_CACHE_CLEANUP_INTERVAL_MS), 10 * 60 * 1000).longValue();

    // if we needed we could make config for update thread pool size
    _threadPoolSize = Utils.getInt(_conf.get(Config.SUPERVISOR_BLOBSTORE_DOWNLOAD_THREAD_COUNT), 5);
    _blobDownloadRetries = Utils.getInt(_conf.get(
            Config.SUPERVISOR_BLOBSTORE_DOWNLOAD_MAX_RETRIES), 3);

    _execService = Executors.newFixedThreadPool(_threadPoolSize);
    _updateExecService = Executors.newFixedThreadPool(_threadPoolSize);
    reconstructLocalizedResources();
  }

  // For testing, it allows setting size in bytes
  protected void setTargetCacheSize(long size) {
    _cacheTargetSize = size;
  }

  // For testing, be careful as it doesn't clone
  ConcurrentMap<String, LocalizedResourceSet> getUserResources() {
    return _userRsrc;
  }

  public void startCleaner() {
    _cacheCleanupService = new ScheduledThreadPoolExecutor(1,
        new ThreadFactoryBuilder()
            .setNameFormat("Localizer Cache Cleanup")
            .build());

    _cacheCleanupService.scheduleWithFixedDelay(new Runnable() {
          @Override
          public void run() {
            handleCacheCleanup();
          }
        }, _cacheCleanupPeriod, _cacheCleanupPeriod, TimeUnit.MILLISECONDS);
  }

  public void shutdown() {
    if (_cacheCleanupService != null) {
      _cacheCleanupService.shutdown();
    }
    if (_execService != null) {
      _execService.shutdown();
    }
    if (_updateExecService != null) {
      _updateExecService.shutdown();
    }
  }

  // baseDir/supervisor/usercache/
  protected File getUserCacheDir() {
    return new File(_localBaseDir, USERCACHE);
  }

  // baseDir/supervisor/usercache/user1/
  protected File getLocalUserDir(String userName) {
    return new File(getUserCacheDir(), userName);
  }

  // baseDir/supervisor/usercache/user1/filecache
  public File getLocalUserFileCacheDir(String userName) {
    return new File(getLocalUserDir(userName), FILECACHE);
  }

  // baseDir/supervisor/usercache/user1/filecache/files
  protected File getCacheDirForFiles(File dir) {
    return new File(dir, FILESDIR);
  }

  // get the directory to put uncompressed archives in
  // baseDir/supervisor/usercache/user1/filecache/archives
  protected File getCacheDirForArchives(File dir) {
    return new File(dir, ARCHIVESDIR);
  }

  protected void addLocalizedResourceInDir(String dir, LocalizedResourceSet lrsrcSet,
      boolean uncompress) {
    File[] lrsrcs = readCurrentBlobs(dir);

    if (lrsrcs != null) {
      for (File rsrc : lrsrcs) {
        LOG.info("add localized in dir found: " + rsrc);
        /// strip off .suffix
        String path = rsrc.getPath();
        int p = path.lastIndexOf('.');
        if (p > 0) {
          path = path.substring(0, p);
        }
        LOG.debug("local file is: {} path is: {}", rsrc.getPath(), path);
        LocalizedResource lrsrc = new LocalizedResource(new File(path).getName(), path,
            uncompress);
        lrsrcSet.addResource(lrsrc.getKey(), lrsrc, uncompress);
      }
    }
  }

  protected File[] readDirContents(String location) {
    File dir = new File(location);
    File[] files = null;
    if (dir.exists()) {
      files = dir.listFiles();
    }
    return files;
  }

  // Looks for files in the directory with .current suffix
  protected File[] readCurrentBlobs(String location) {
    File dir = new File(location);
    File[] files = null;
    if (dir.exists()) {
      files = dir.listFiles(new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
          return name.toLowerCase().endsWith(Utils.DEFAULT_CURRENT_BLOB_SUFFIX);
        }
      });
    }
    return files;
  }

  // Check to see if there are any existing files already localized.
  protected void reconstructLocalizedResources() {
    try {
      LOG.info("Reconstruct localized resource: " + getUserCacheDir().getPath());
      File[] users = readDirContents(getUserCacheDir().getPath());

      if (users != null) {
        for (File userDir : users) {
          String user = userDir.getName();
          LOG.debug("looking in: {} for user: {}", userDir.getPath(), user);
          LocalizedResourceSet newSet = new LocalizedResourceSet(user);
          LocalizedResourceSet lrsrcSet = _userRsrc.putIfAbsent(user, newSet);
          if (lrsrcSet == null) {
            lrsrcSet = newSet;
          }
          addLocalizedResourceInDir(getCacheDirForFiles(getLocalUserFileCacheDir(user)).getPath(),
              lrsrcSet, false);
          addLocalizedResourceInDir(
              getCacheDirForArchives(getLocalUserFileCacheDir(user)).getPath(),
              lrsrcSet, true);
        }
      } else {
        LOG.warn("No left over resources found for any user during reconstructing of local resources at: {}", getUserCacheDir().getPath());
      }
    } catch (Exception e) {
      LOG.error("ERROR reconstructing localized resources", e);
    }
  }

  // ignores invalid user/topo/key
  public synchronized void removeBlobReference(String key, String user, String topo,
      boolean uncompress) throws AuthorizationException, KeyNotFoundException {
    LocalizedResourceSet lrsrcSet = _userRsrc.get(user);
    if (lrsrcSet != null) {
      LocalizedResource lrsrc = lrsrcSet.get(key, uncompress);
      if (lrsrc != null) {
        LOG.debug("removing blob reference to: {} for topo: {}", key, topo);
        lrsrc.removeReference(topo);
      } else {
        LOG.warn("trying to remove non-existent blob, key: " + key + " for user: " + user +
            " topo: " + topo);
      }
    } else {
      LOG.warn("trying to remove blob for non-existent resource set for user: " + user + " key: "
          + key + " topo: " + topo);
    }
  }

  public synchronized void addReferences(List<LocalResource> localresource, String user,
       String topo) {
    LocalizedResourceSet lrsrcSet = _userRsrc.get(user);
    if (lrsrcSet != null) {
      for (LocalResource blob : localresource) {
        LocalizedResource lrsrc = lrsrcSet.get(blob.getBlobName(), blob.shouldUncompress());
        if (lrsrc != null) {
          lrsrc.addReference(topo);
          LOG.debug("added reference for topo: {} key: {}", topo, blob);
        } else {
          LOG.warn("trying to add reference to non-existent blob, key: " + blob + " topo: " + topo);
        }
      }
    } else {
      LOG.warn("trying to add reference to non-existent local resource set, " +
          "user: " + user + " topo: " + topo);
    }
  }

  /**
   * This function either returns the blob in the existing cache or if it doesn't exist in the
   * cache, it will download the blob and will block until the download is complete.
   */
  public LocalizedResource getBlob(LocalResource localResource, String user, String topo,
       File userFileDir) throws AuthorizationException, KeyNotFoundException, IOException {
    ArrayList<LocalResource> arr = new ArrayList<LocalResource>();
    arr.add(localResource);
    List<LocalizedResource> results = getBlobs(arr, user, topo, userFileDir);
    if (results.isEmpty() || results.size() != 1) {
      throw new IOException("Unknown error getting blob: " + localResource + ", for user: " + user +
          ", topo: " + topo);
    }
    return results.get(0);
  }

  protected boolean isLocalizedResourceDownloaded(LocalizedResource lrsrc) {
    File rsrcFileCurrent = new File(lrsrc.getCurrentSymlinkPath());
    File rsrcFileWithVersion = new File(lrsrc.getFilePathWithVersion());
    File versionFile = new File(lrsrc.getVersionFilePath());
    return (rsrcFileWithVersion.exists() && rsrcFileCurrent.exists() && versionFile.exists());
  }

  protected boolean isLocalizedResourceUpToDate(LocalizedResource lrsrc,
      ClientBlobStore blobstore) throws AuthorizationException, KeyNotFoundException {
    String localFile = lrsrc.getFilePath();
    long nimbusBlobVersion = Utils.nimbusVersionOfBlob(lrsrc.getKey(), blobstore);
    long currentBlobVersion = Utils.localVersionOfBlob(localFile);
    return (nimbusBlobVersion == currentBlobVersion);
  }

  protected ClientBlobStore getClientBlobStore() {
    return Utils.getClientBlobStoreForSupervisor(_conf);
  }

  /**
   * This function updates blobs on the supervisor. It uses a separate thread pool and runs
   * asynchronously of the download and delete.
   */
  public List<LocalizedResource> updateBlobs(List<LocalResource> localResources,
       String user) throws AuthorizationException, KeyNotFoundException, IOException {
    LocalizedResourceSet lrsrcSet = _userRsrc.get(user);
    ArrayList<LocalizedResource> results = new ArrayList<>();
    ArrayList<Callable<LocalizedResource>> updates = new ArrayList<>();

    if (lrsrcSet == null) {
      // resource set must have been removed
      return results;
    }
    ClientBlobStore blobstore = null;
    try {
      blobstore = getClientBlobStore();
      for (LocalResource localResource: localResources) {
        String key = localResource.getBlobName();
        LocalizedResource lrsrc = lrsrcSet.get(key, localResource.shouldUncompress());
        if (lrsrc == null) {
          LOG.warn("blob requested for update doesn't exist: {}", key);
          continue;
        } else {
          // update it if either the version isn't the latest or if any local blob files are missing
          if (!isLocalizedResourceUpToDate(lrsrc, blobstore) ||
              !isLocalizedResourceDownloaded(lrsrc)) {
            LOG.debug("updating blob: {}", key);
            updates.add(new DownloadBlob(this, _conf, key, new File(lrsrc.getFilePath()), user,
                lrsrc.isUncompressed(), true));
          }
        }
      }
    } finally {
      if(blobstore != null) {
        blobstore.shutdown();
      }
    }
    try {
      List<Future<LocalizedResource>> futures = _updateExecService.invokeAll(updates);
      for (Future<LocalizedResource> futureRsrc : futures) {
        try {
          LocalizedResource lrsrc = futureRsrc.get();
          // put the resource just in case it was removed at same time by the cleaner
          LocalizedResourceSet newSet = new LocalizedResourceSet(user);
          LocalizedResourceSet newlrsrcSet = _userRsrc.putIfAbsent(user, newSet);
          if (newlrsrcSet == null) {
            newlrsrcSet = newSet;
          }
          newlrsrcSet.updateResource(lrsrc.getKey(), lrsrc, lrsrc.isUncompressed());
          results.add(lrsrc);
        }
        catch (ExecutionException e) {
          LOG.error("Error updating blob: ", e);
          if (e.getCause() instanceof AuthorizationException) {
            throw (AuthorizationException)e.getCause();
          }
          if (e.getCause() instanceof KeyNotFoundException) {
            throw (KeyNotFoundException)e.getCause();
          }
        }
      }
    } catch (RejectedExecutionException re) {
      LOG.error("Error updating blobs : ", re);
    } catch (InterruptedException ie) {
      throw new IOException("Interrupted Exception", ie);
    }
    return results;
  }

  /**
   * This function either returns the blobs in the existing cache or if they don't exist in the
   * cache, it downloads them in parallel (up to SUPERVISOR_BLOBSTORE_DOWNLOAD_THREAD_COUNT)
   * and will block until all of them have been downloaded
   */
  public synchronized List<LocalizedResource> getBlobs(List<LocalResource> localResources,
      String user, String topo, File userFileDir)
      throws AuthorizationException, KeyNotFoundException, IOException {

    LocalizedResourceSet newSet = new LocalizedResourceSet(user);
    LocalizedResourceSet lrsrcSet = _userRsrc.putIfAbsent(user, newSet);
    if (lrsrcSet == null) {
      lrsrcSet = newSet;
    }
    ArrayList<LocalizedResource> results = new ArrayList<>();
    ArrayList<Callable<LocalizedResource>> downloads = new ArrayList<>();

    ClientBlobStore blobstore = null;
    try {
      blobstore = getClientBlobStore();
      for (LocalResource localResource: localResources) {
        String key = localResource.getBlobName();
        boolean uncompress = localResource.shouldUncompress();
        LocalizedResource lrsrc = lrsrcSet.get(key, localResource.shouldUncompress());
        boolean isUpdate = false;
        if ((lrsrc != null) && (lrsrc.isUncompressed() == localResource.shouldUncompress()) &&
            (isLocalizedResourceDownloaded(lrsrc))) {
          if (isLocalizedResourceUpToDate(lrsrc, blobstore)) {
            LOG.debug("blob already exists: {}", key);
            lrsrc.addReference(topo);
            results.add(lrsrc);
            continue;
          }
          LOG.debug("blob exists but isn't up to date: {}", key);
          isUpdate = true;
        }

        // go off to blobstore and get it
        // assume dir passed in exists and has correct permission
        LOG.debug("fetching blob: {}", key);
        File downloadDir = getCacheDirForFiles(userFileDir);
        File localFile = new File(downloadDir, key);
        if (uncompress) {
          // for compressed file, download to archives dir
          downloadDir = getCacheDirForArchives(userFileDir);
          localFile = new File(downloadDir, key);
        }
        downloadDir.mkdir();
        downloads.add(new DownloadBlob(this, _conf, key, localFile, user, uncompress,
            isUpdate));
      }
    } finally {
      if(blobstore !=null) {
        blobstore.shutdown();
      }
    }
    try {
      List<Future<LocalizedResource>> futures = _execService.invokeAll(downloads);
      for (Future<LocalizedResource> futureRsrc: futures) {
        LocalizedResource lrsrc = futureRsrc.get();
        lrsrc.addReference(topo);
        lrsrcSet.addResource(lrsrc.getKey(), lrsrc, lrsrc.isUncompressed());
        results.add(lrsrc);
      }
    } catch (ExecutionException e) {
      if (e.getCause() instanceof AuthorizationException)
        throw (AuthorizationException)e.getCause();
      else if (e.getCause() instanceof KeyNotFoundException) {
        throw (KeyNotFoundException)e.getCause();
      } else {
        throw new IOException("Error getting blobs", e);
      }
    } catch (RejectedExecutionException re) {
      throw new IOException("RejectedExecutionException: ", re);
    } catch (InterruptedException ie) {
      throw new IOException("Interrupted Exception", ie);
    }
    return results;
  }

  static class DownloadBlob implements Callable<LocalizedResource> {

    private Localizer _localizer;
    private Map _conf;
    private String _key;
    private File _localFile;
    private String _user;
    private boolean _uncompress;
    private boolean _isUpdate;

    public DownloadBlob(Localizer localizer, Map conf, String key, File localFile,
        String user, boolean uncompress, boolean update) {
      _localizer = localizer;
      _conf = conf;
      _key = key;
      _localFile = localFile;
      _user = user;
      _uncompress = uncompress;
      _isUpdate = update;
    }

    @Override
    public LocalizedResource call()
        throws AuthorizationException, KeyNotFoundException, IOException  {
      return _localizer.downloadBlob(_conf, _key, _localFile, _user, _uncompress,
        _isUpdate);
    }
  }

  private LocalizedResource downloadBlob(Map conf, String key, File localFile,
      String user, boolean uncompress, boolean isUpdate)
      throws AuthorizationException, KeyNotFoundException, IOException {
    ClientBlobStore blobstore = null;
    try {
      blobstore = getClientBlobStore();
      long nimbusBlobVersion = Utils.nimbusVersionOfBlob(key, blobstore);
      long oldVersion = Utils.localVersionOfBlob(localFile.toString());
      FileOutputStream out = null;
      PrintWriter writer = null;
      int numTries = 0;
      String localizedPath = localFile.toString();
      String localFileWithVersion = Utils.constructBlobWithVersionFileName(localFile.toString(),
              nimbusBlobVersion);
      String localVersionFile = Utils.constructVersionFileName(localFile.toString());
      String downloadFile = localFileWithVersion;
      if (uncompress) {
        // we need to download to temp file and then unpack into the one requested
        downloadFile = new File(localFile.getParent(), TO_UNCOMPRESS + localFile.getName()).toString();
      }
      while (numTries < _blobDownloadRetries) {
        out = new FileOutputStream(downloadFile);
        numTries++;
        try {
          if (!Utils.canUserReadBlob(blobstore.getBlobMeta(key), user)) {
            throw new AuthorizationException(user + " does not have READ access to " + key);
          }
          InputStreamWithMeta in = blobstore.getBlob(key);
          byte[] buffer = new byte[1024];
          int len;
          while ((len = in.read(buffer)) >= 0) {
            out.write(buffer, 0, len);
          }
          out.close();
          in.close();
          if (uncompress) {
            Utils.unpack(new File(downloadFile), new File(localFileWithVersion));
            LOG.debug("uncompressed " + downloadFile + " to: " + localFileWithVersion);
          }

          // Next write the version.
          LOG.info("Blob: " + key + " updated with new Nimbus-provided version: " +
              nimbusBlobVersion + " local version was: " + oldVersion);
          // The false parameter ensures overwriting the version file, not appending
          writer = new PrintWriter(
              new BufferedWriter(new FileWriter(localVersionFile, false)));
          writer.println(nimbusBlobVersion);
          writer.close();

          try {
            setBlobPermissions(conf, user, localFileWithVersion);
            setBlobPermissions(conf, user, localVersionFile);

            // Update the key.current symlink. First create tmp symlink and do
            // move of tmp to current so that the operation is atomic.
            String tmp_uuid_local = java.util.UUID.randomUUID().toString();
            LOG.debug("Creating a symlink @" + localFile + "." + tmp_uuid_local + " , " +
                "linking to: " + localFile + "." + nimbusBlobVersion);
            File uuid_symlink = new File(localFile + "." + tmp_uuid_local);

            Files.createSymbolicLink(uuid_symlink.toPath(),
                Paths.get(Utils.constructBlobWithVersionFileName(localFile.toString(),
                        nimbusBlobVersion)));
            File current_symlink = new File(Utils.constructBlobCurrentSymlinkName(
                    localFile.toString()));
            Files.move(uuid_symlink.toPath(), current_symlink.toPath(), ATOMIC_MOVE);
          } catch (IOException e) {
            // if we fail after writing the version file but before we move current link we need to
            // restore the old version to the file
            try {
              PrintWriter restoreWriter = new PrintWriter(
                  new BufferedWriter(new FileWriter(localVersionFile, false)));
              restoreWriter.println(oldVersion);
              restoreWriter.close();
            } catch (IOException ignore) {}
            throw e;
          }

          String oldBlobFile = localFile + "." + oldVersion;
          try {
            // Remove the old version. Note that if a number of processes have that file open,
            // the OS will keep the old blob file around until they all close the handle and only
            // then deletes it. No new process will open the old blob, since the users will open the
            // blob through the "blob.current" symlink, which always points to the latest version of
            // a blob. Remove the old version after the current symlink is updated as to not affect
            // anyone trying to read it.
            if ((oldVersion != -1) && (oldVersion != nimbusBlobVersion)) {
              LOG.info("Removing an old blob file:" + oldBlobFile);
              Files.delete(Paths.get(oldBlobFile));
            }
          } catch (IOException e) {
            // At this point we have downloaded everything and moved symlinks.  If the remove of
            // old fails just log an error
            LOG.error("Exception removing old blob version: " + oldBlobFile);
          }

          break;
        } catch (AuthorizationException ae) {
          // we consider this non-retriable exceptions
          if (out != null) {
            out.close();
          }
          new File(downloadFile).delete();
          throw ae;
        } catch (IOException | KeyNotFoundException e) {
          if (out != null) {
            out.close();
          }
          if (writer != null) {
            writer.close();
          }
          new File(downloadFile).delete();
          if (uncompress) {
            try {
              FileUtils.deleteDirectory(new File(localFileWithVersion));
            } catch (IOException ignore) {}
          }
          if (!isUpdate) {
            // don't want to remove existing version file if its an update
            new File(localVersionFile).delete();
          }

          if (numTries < _blobDownloadRetries) {
            LOG.error("Failed to download blob, retrying", e);
          } else {
            throw e;
          }
        }
      }
      return new LocalizedResource(key, localizedPath, uncompress);
    } finally {
      if(blobstore != null) {
        blobstore.shutdown();
      }
    }
  }

  public void setBlobPermissions(Map conf, String user, String path)
      throws IOException {

    if (!Utils.getBoolean(conf.get(Config.SUPERVISOR_RUN_WORKER_AS_USER), false)) {
      return;
    }
    String wlCommand = Utils.getString(conf.get(Config.SUPERVISOR_WORKER_LAUNCHER), "");
    if (wlCommand.isEmpty()) {
      String stormHome = System.getProperty("storm.home");
      wlCommand = stormHome + "/bin/worker-launcher";
    }
    List<String> command = new ArrayList<String>(Arrays.asList(wlCommand, user, "blob", path));

    String[] commandArray = command.toArray(new String[command.size()]);
    ShellCommandExecutor shExec = new ShellCommandExecutor(commandArray);
    LOG.info("Setting blob permissions, command: {}", Arrays.toString(commandArray));

    try {
      shExec.execute();
      LOG.debug("output: {}", shExec.getOutput());
    } catch (ExitCodeException e) {
      int exitCode = shExec.getExitCode();
      LOG.warn("Exit code from worker-launcher is : " + exitCode, e);
      LOG.debug("output: {}", shExec.getOutput());
      throw new IOException("Setting blob permissions failed" +
          " (exitCode=" + exitCode + ") with output: " + shExec.getOutput(), e);
    }
  }


  public synchronized void handleCacheCleanup() {
    LocalizedResourceRetentionSet toClean = new LocalizedResourceRetentionSet(_cacheTargetSize);
    // need one large set of all and then clean via LRU
    for (LocalizedResourceSet t : _userRsrc.values()) {
      toClean.addResources(t);
      LOG.debug("Resources to be cleaned after adding {} : {}", t.getUser(), toClean);
    }
    toClean.cleanup();
    LOG.debug("Resource cleanup: {}", toClean);
    for (LocalizedResourceSet t : _userRsrc.values()) {
      if (t.getSize() == 0) {
        String user = t.getUser();

        LOG.debug("removing empty set: {}", user);
        File userFileCacheDir = getLocalUserFileCacheDir(user);
        getCacheDirForFiles(userFileCacheDir).delete();
        getCacheDirForArchives(userFileCacheDir).delete();
        getLocalUserFileCacheDir(user).delete();
        boolean dirsRemoved = getLocalUserDir(user).delete();
        // to catch race with update thread
        if (dirsRemoved) {
          _userRsrc.remove(user);
        }
      }
    }
  }
}
