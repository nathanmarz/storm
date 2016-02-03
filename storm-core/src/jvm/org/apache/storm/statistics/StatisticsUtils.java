package org.apache.storm.statistics;

import org.apache.storm.Config;
import org.apache.storm.statistics.reporters.JmxPreparableReporter;
import org.apache.storm.statistics.reporters.PreparableReporter;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class StatisticsUtils {
    private final static Logger LOG = LoggerFactory.getLogger(StatisticsUtils.class);

    public static List<PreparableReporter> getPreparableReporters(Map stormConf) {
        PreparableReporter reporter = new JmxPreparableReporter();
        List<String> clazzes = (List<String>) stormConf.get(Config.STORM_STATISTICS_PREPARABLE_REPORTER_PLUGIN);
        List<PreparableReporter> reporterList = new ArrayList<>();

        if (clazzes != null) {
            for(String clazz: clazzes ) {
                reporterList.add(getPreparableReporter(clazz));
            }
        }
        if(reporterList.isEmpty()) {
            reporterList.add(new JmxPreparableReporter());
        }
        return reporterList;
    }

    private static PreparableReporter getPreparableReporter(String clazz) {
        PreparableReporter reporter = null;
        LOG.info("Using statistics reporter plugin:" + clazz);
        if(clazz != null) {
            reporter = (PreparableReporter) Utils.newInstance(clazz);
        }
        return reporter;
    }
}
