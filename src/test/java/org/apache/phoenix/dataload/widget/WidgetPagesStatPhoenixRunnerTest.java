package org.apache.phoenix.dataload.widget;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.phoenix.dataload.widget.WidgetPagesStatLineParser;
import org.apache.phoenix.dataload.widget.WidgetPagesStatPhoenixLoader;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;
import java.sql.Connection;

/**
 * Created by thangar
 */
public class WidgetPagesStatPhoenixRunnerTest {

    // We use HBaseTestUtil because we need to start up a MapReduce cluster as well
    //private static HBaseTestingUtility hbaseTestUtil;
    private static String zkQuorum;
    private static Connection conn;

    @BeforeClass
    public static void setUp() throws Exception {
//        hbaseTestUtil = new HBaseTestingUtility();
//        Configuration conf = hbaseTestUtil.getConfiguration();
//        //setUpConfigForMiniCluster(conf);
//        hbaseTestUtil.startMiniCluster();
//        hbaseTestUtil.startMiniMapReduceCluster();
//
//        Class.forName(PhoenixDriver.class.getName());
//        zkQuorum = "localhost:" + hbaseTestUtil.getZkCluster().getClientPort();
//        conn = DriverManager.getConnection(PhoenixRuntime.JDBC_PROTOCOL
//                + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + zkQuorum);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
//        try {
//            conn.close();
//        } finally {
//            try {
//                PhoenixDriver.INSTANCE.close();
//            } finally {
//                try {
//                    DriverManager.deregisterDriver(PhoenixDriver.INSTANCE);
//                } finally {
//                    try {
//                        hbaseTestUtil.shutdownMiniMapReduceCluster();
//                    } finally {
//                        hbaseTestUtil.shutdownMiniCluster();
//                    }
//                }
//            }
//        }
    }

    @Test
    public void runPhoenixSeed() throws InterruptedException, IOException, ClassNotFoundException {
        final Configuration conf = new Configuration();
        conf.set("fs.default.name", "file:////");

        HBaseConfiguration.addHbaseResources(conf);
        String rundateString = "20150628";
        conf.set("rundate", rundateString);

        long timestamp = WidgetPagesStatLineParser.parseDateString(rundateString);
        conf.set(PhoenixRuntime.CURRENT_SCN_ATTRIB, ""+(timestamp));
        conf.set(PhoenixConfigurationUtil.CURRENT_SCN_VALUE, "" + timestamp);

        //URL resource = getClass().getClassLoader().getResource("widget-input-test.txt");
        Path path = new Path("/Users/thangar/pages-segment-widget-top100/widget-pages-segment_top100/000000_0");
        WidgetPagesStatPhoenixLoader.createJob(conf, path, "WIDGET_PAGES_STAT_DEBUG");
    }


    @Test
    public void runPhoenixSeedWithBadRecords() throws InterruptedException, IOException, ClassNotFoundException {
        final Configuration conf = new Configuration();
        conf.set("fs.default.name", "file:////");

        HBaseConfiguration.addHbaseResources(conf);

        String rundateString = "20151110";
        conf.set("rundate", rundateString);

        URL resource = getClass().getClassLoader().getResource("widget-input-badrecords.txt");
        //Path path = new Path("/Users/thangar/pages-segment-widget-top100/widget-pages-segment_top100/000000_0");
        WidgetPagesStatPhoenixLoader.createJob(conf, new Path(resource.getPath()), "WIDGET_PAGES_STAT");
    }
}
