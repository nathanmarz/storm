package org.apache.storm.solr.util;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by hlouro on 7/31/15.
 */
public class TestUtil {
    public static String getDate() {
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        String date = df.format(new Date());
        System.out.println(date);
        return date;
    }
}
