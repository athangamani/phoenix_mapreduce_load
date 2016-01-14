package org.apache.phoenix.dataload.widget;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.phoenix.mapreduce.PhoenixOutputFormat;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.mapreduce.util.PhoenixMapReduceUtil;
import org.apache.phoenix.util.PhoenixRuntime;

import java.io.IOException;

/**
 * Created by thangar on 9/3/15.
 */
public class WidgetPagesStatPhoenixLoader {

    public static final String NAME = "WidgetPagesStatPhoenixLoader";
    private static int count = 0;
    private static final double id = Math.random();

    public static class WidgetPhoenixMapper extends Mapper<LongWritable, Text, NullWritable, WidgetPagesStatWritable> {
        @Override
        public void map(LongWritable longWritable, Text text, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String rundateString = conf.get("rundate");
            WidgetPagesStatLineParser parser = new WidgetPagesStatLineParser();
            try {
                WidgetPagesStat widgetPagesStat = parser.parse(text.toString());

                if (widgetPagesStat != null && primaryKeysNotEmpty(widgetPagesStat)) {
                    WidgetPagesStatWritable widgetPagesStatWritable = new WidgetPagesStatWritable();
                    WidgetPagesStat widgetPagesStats = new WidgetPagesStat();

                    widgetPagesStats.setWebId(widgetPagesStat.getWebId());
                    widgetPagesStats.setWebPageLabel(widgetPagesStat.getWebPageLabel());
                    widgetPagesStats.setWidgetInstanceId(widgetPagesStat.getWidgetInstanceId());
                    widgetPagesStats.setWidgetType(widgetPagesStat.getWidgetType());
                    widgetPagesStats.setWidgetVersion(widgetPagesStat.getWidgetVersion());
                    widgetPagesStats.setWidgetContext(widgetPagesStat.getWidgetContext());

                    widgetPagesStats.setTotalClicks(widgetPagesStat.getTotalClicks());
                    widgetPagesStats.setTotalClickViews(widgetPagesStat.getTotalClickViews());
                    widgetPagesStats.setTotalHoverTime(widgetPagesStat.getTotalHoverTime());
                    widgetPagesStats.setTotalViewableTime(widgetPagesStat.getTotalViewableTime());
                    widgetPagesStats.setTotalTimeOnPage(widgetPagesStat.getTotalTimeOnPage());
                    widgetPagesStats.setViewCount(widgetPagesStat.getViewCount());

                    widgetPagesStats.setDeviceType(widgetPagesStat.getDeviceType());
                    widgetPagesStats.setViewCount(widgetPagesStat.getViewCount());
                    long timestamp = WidgetPagesStatLineParser.parseDateString(rundateString);
                    //System.out.println("timestamp = " + timestamp);
                    widgetPagesStats.setViewDateTimestamp(timestamp);
                    widgetPagesStats.setViewDate(rundateString);
                    widgetPagesStats.setUserSegments(widgetPagesStat.getUserSegments());

                    widgetPagesStats.setDimDateKey(widgetPagesStat.getDimDateKey());
                    widgetPagesStats.setRowNumber(widgetPagesStat.getRowNumber());

                    widgetPagesStatWritable.setWidgetPagesStat(widgetPagesStats);
                    context.write(NullWritable.get(), widgetPagesStatWritable);
                    //System.out.println("id = " + id + " count = " + count);
                    count++;
                }

            }catch (Exception e){
                e.printStackTrace();
            }
        }

        private boolean primaryKeysNotEmpty(WidgetPagesStat widgetPagesStat) {
            return widgetPagesStat.getWebId() != null && !("").equals(widgetPagesStat.getWebId())
                    && widgetPagesStat.getWebPageLabel() != null && !("").equals(widgetPagesStat.getWebPageLabel());
        }
    }

    public static void main(String[] args) throws Exception {
        final Configuration conf = new Configuration();

        if (args != null && args.length != 4){
            throw new Exception("Usage : HDFSInputPath zookeeperQuorum-CommaSeperated TableName InputDataTime-Format-yyyyMMdd");
        }

        String inputpath = args[0];
        String zookeeperQuorum = args[2];
        String tableName = args[3];
        String rundateString = args[4];

        conf.set("rundate", rundateString);
        long timestamp = WidgetPagesStatLineParser.parseDateString(rundateString);
        conf.set(PhoenixRuntime.CURRENT_SCN_ATTRIB, ""+(timestamp));
        conf.set(PhoenixConfigurationUtil.CURRENT_SCN_VALUE, "" + timestamp);

        HBaseConfiguration.addHbaseResources(conf);
        conf.set("zookeeper.znode.parent", "/hbase-unsecure");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        //String nameNodeService = args[3];
        //conf.set("fs.defaultFS", "hdfs://" + nameNodeService);
        conf.set("hbase.zookeeper.quorum", zookeeperQuorum);
        //conf.set("mapreduce.framework.name", "yarn");
        conf.set("zookeeper.znode.parent", "/hbase-unsecure");

        //conf.set("yarn.resourcemanager.address", args[5]);
        //conf.set("yarn.resourcemanager.scheduler.address", args[6]);
        //conf.set("dfs.nameservices",nameNodeService);
        //conf.set("dfs.ha.namenodes."+nameNodeService, "namenode1,namenode2");
        //conf.set("dfs.namenode.rpc-address." +nameNodeService+ ".namenode1", args[7]);
        //conf.set("dfs.namenode.rpc-address." +nameNodeService+ ".namenode2", args[8]);
        //conf.set("dfs.client.failover.proxy.provider."+nameNodeService,"org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        //conf.set("yarn.application.classpath",
        //        "/usr/hdp/current/hadoop-client/*,/usr/hdp/current/hadoop-client/lib/*," +
        //                "/usr/hdp/current/hadoop-hdfs-client/*,/usr/hdp/current/hadoop-hdfs-client/lib/*," +
        //                "/usr/hdp/current/hadoop-yarn-client/*,/usr/hdp/current/hadoop-yarn-client/lib/*," +
        //                "/usr/hdp/current/hive/lib/*,/usr/hdp/current/hadoop-mapreduce-client/*," +
        //                "/usr/hdp/current/hadoop-mapreduce-client/lib/*, /usr/hdp/current/phoenix-client/lib/*," +
        //                "/usr/hdp/2.2.4.2-2/hadoop-mapreduce/*");

        Path inputPath = new Path(inputpath);
        createJob(conf, inputPath, tableName);
    }

    public static void createJob(Configuration conf, Path inputPath, String tableName) throws IOException, InterruptedException, ClassNotFoundException {
        Job job = Job.getInstance(conf, NAME);
        // Set the target Phoenix table and the columns
        PhoenixMapReduceUtil.setOutput(job, tableName, "WEB_ID,WEB_PAGE_LABEL,DEVICE_TYPE," +
                "WIDGET_INSTANCE_ID,WIDGET_TYPE,WIDGET_VERSION,WIDGET_CONTEXT," +
                "TOTAL_CLICKS,TOTAL_CLICK_VIEWS,TOTAL_HOVER_TIME_MS,TOTAL_TIME_ON_PAGE_MS,TOTAL_VIEWABLE_TIME_MS," +
                "VIEW_COUNT,USER_SEGMENT,DIM_DATE_KEY,VIEW_DATE,VIEW_DATE_TIMESTAMP,ROW_NUMBER");
        FileInputFormat.setInputPaths(job, inputPath);
        job.setMapperClass(WidgetPhoenixMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(WidgetPagesStatWritable.class);
        job.setOutputFormatClass(PhoenixOutputFormat.class);
        TableMapReduceUtil.addDependencyJars(job);
        job.setNumReduceTasks(0);
        job.waitForCompletion(true);
        if (!job.waitForCompletion(true)){
            throw new RuntimeException("Widget PhoenixHbase Upload Job Failed or Timed out");
        }
    }
}
