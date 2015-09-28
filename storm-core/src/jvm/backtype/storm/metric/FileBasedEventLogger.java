package backtype.storm.metric;

import backtype.storm.task.TopologyContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class FileBasedEventLogger implements IEventLogger {
    private static Logger LOG = LoggerFactory.getLogger(FileBasedEventLogger.class);

    private static final int FLUSH_INTERVAL_MILLIS = 1000;

    private Path eventLogPath;
    private BufferedWriter eventLogWriter;
    private volatile boolean dirty = false;

    private void initLogWriter(Path logFilePath) {
        try {
            LOG.info("logFilePath {}", logFilePath);
            eventLogPath = logFilePath;
            eventLogWriter = Files.newBufferedWriter(eventLogPath, StandardCharsets.UTF_8, StandardOpenOption.CREATE,
                                                     StandardOpenOption.WRITE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            LOG.error("Error setting up FileBasedEventLogger.", e);
            throw new RuntimeException(e);
        }
    }


    private void setUpFlushTask() {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        Runnable task = new Runnable() {
            @Override
            public void run() {
                try {
                    if(dirty) {
                        eventLogWriter.flush();
                        dirty = false;
                    }
                } catch (IOException ex) {
                    LOG.error("Error flushing " + eventLogPath, ex);
                    throw new RuntimeException(ex);
                }
            }
        };

        scheduler.scheduleAtFixedRate(task, FLUSH_INTERVAL_MILLIS, FLUSH_INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
    }


    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        String logDir;
        String stormId = context.getStormId();
        int port = context.getThisWorkerPort();
        if((logDir = System.getProperty("storm.log.dir")) == null
                && (logDir = System.getProperty("java.io.tmpdir")) == null) {
            String msg = "Could not determine the directory to log events.";
            LOG.error(msg);
            throw new RuntimeException(msg);
        } else {
            LOG.info("FileBasedEventLogger log directory {}.", logDir);
        }

        /*
         * Include the topology name & worker port in the file name so that
         * multiple event loggers can log independently.
         */
        initLogWriter(Paths.get(logDir, String.format("%s-events-%d.log", stormId, port)));
        setUpFlushTask();
    }

    @Override
    public void log(EventInfo event) {
        try {
            //TODO: file rotation
            eventLogWriter.write(event.toString());
            eventLogWriter.newLine();
            dirty = true;
        } catch (IOException ex) {
            LOG.error("Error logging event {}", event, ex);
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void close() {
        try {
            eventLogWriter.close();
        } catch (IOException ex) {
            LOG.error("Error closing event log.", ex);
        }
    }
}
