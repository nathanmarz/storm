package org.apache.storm.statistics;

import org.apache.storm.Config;
import org.apache.storm.statistics.reporters.JMXPreparableReporter;
import org.apache.storm.statistics.reporters.PreparableReporter;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class StatisticsUtils {
    private final static Logger LOG = LoggerFactory.getLogger(StatisticsUtils.class);

    public static PreparableReporter getPreparableReporter(Map stormConf) {
        PreparableReporter reporter = new JMXPreparableReporter();
        String clazz = (String) stormConf.get(Config.STORM_STATISTICS_PREPARABLE_REPORTER_PLUGIN);
        LOG.info("Using statistics reporter plugin:" + clazz);
        if(clazz != null) {
            reporter = (PreparableReporter) Utils.newInstance(clazz);
        } else {
            reporter = new JMXPreparableReporter();
        }
        return reporter;
    }
}
