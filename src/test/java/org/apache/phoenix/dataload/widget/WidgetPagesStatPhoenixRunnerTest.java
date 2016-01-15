package org.apache.phoenix.dataload.widget;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;

/**
 * Created by thangar
 */
public class WidgetPagesStatPhoenixRunnerTest {

    @Test
    public void runPhoenixSeed() throws InterruptedException, IOException, ClassNotFoundException {
        final Configuration conf = new Configuration();
        conf.set("fs.default.name", "file:////");

        HBaseConfiguration.addHbaseResources(conf);
        String rundateString = "20160101";
        conf.set("rundate", rundateString);

        long timestamp = WidgetPagesStatLineParser.parseDateString(rundateString);
        conf.set(PhoenixRuntime.CURRENT_SCN_ATTRIB, ""+(timestamp));
        conf.set(PhoenixConfigurationUtil.CURRENT_SCN_VALUE, "" + timestamp);

        URL resource = getClass().getClassLoader().getResource("widget-input-test.txt");
        //Path path = new Path("data-1.txt");
        WidgetPagesStatPhoenixLoader.createJob(conf, new Path(resource.getPath()), "WIDGET_PAGES_STAT");
    }


    @Test
    public void runPhoenixSeedWithBadRecords() throws InterruptedException, IOException, ClassNotFoundException {
        final Configuration conf = new Configuration();
        conf.set("fs.default.name", "file:////");

        HBaseConfiguration.addHbaseResources(conf);

        String rundateString = "20160101";
        conf.set("rundate", rundateString);

        URL resource = getClass().getClassLoader().getResource("widget-input-badrecords.txt");
        //Path path = new Path("/Users/thangar/pages-segment-widget-top100/widget-pages-segment_top100/000000_0");
        WidgetPagesStatPhoenixLoader.createJob(conf, new Path(resource.getPath()), "WIDGET_PAGES_STAT");
    }
}
