package org.apache.phoenix.dataload.widget;

import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.util.PhoenixRuntime;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Properties;

/**
 * Created by thangar on 1/14/16.
 */
public class WidgetPagesStatUpsertLoadTest {

    public static final int TOTAL_NUMBER_OF_THREADS = 1;
    public static final int TOTAL_ITERATIONS_PER_THREAD = 500000;
    public static final int COMMIT_BATCH_SIZE = 10000;

    @Test
    public void testLoad() throws SQLException, InterruptedException {
        long timestampForConnection = 1447574400000L;
        //long timestampForConnection = 1448092800000;
        //long timestampForConnection = 1447574400000L;

        Collection<Thread> threadBagReference = new LinkedList<Thread>();
        for (int i = 0; i < TOTAL_NUMBER_OF_THREADS; i++) {
            System.out.println("kicking off thread number = " + i);
            Thread executionThread = new Thread(new PhoenixClientThread(i, timestampForConnection));
            executionThread.start();
            threadBagReference.add(executionThread);
        }
        for (Thread thread : threadBagReference) {
            thread.join();
        }
    }

    private static class PhoenixClientThread implements Runnable {
        private int threadNumber;
        private long totalTime;
        private long timestamp;

        public PhoenixClientThread(int threadNumber, long timestamp) {
            this.threadNumber = threadNumber;
            this.timestamp = timestamp;
        }

        public void run() {
            try {
                DriverManager.registerDriver(PhoenixDriver.INSTANCE);
                Connection connection = DriverManager.getConnection("jdbc:phoenix:master01.preprod.datalake.cdk.com:2181:/hbase-unsecure"); //
                    updateData(connection, timestamp, TOTAL_ITERATIONS_PER_THREAD);
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        public void updateData(Connection conn, long ts, int count) throws SQLException {
            long start_timestamp = new java.util.Date().getTime();
            PreparedStatement pstmt = null;
            Properties props = new Properties();
            props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));

            try {
                pstmt = conn.prepareStatement("upsert into WIDGET_PAGES_STAT values " +
                        "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
                for (int seed = 1; seed <= count; seed++) {
                    //System.out.println("seed = " + seed);
                    pstmt.setString(1, "webid" + seed % 10000);
                    pstmt.setString(2, "pagelabel" + seed % 10000);
                    pstmt.setString(3, "deviceid" + seed % 10000);
                    pstmt.setLong(4, seed % 10000);
                    pstmt.setString(5, "widgetype" + seed % 10000);
                    pstmt.setLong(6, seed % 10000);
                    pstmt.setString(7, "widgetcontext" + seed % 10000);

                    pstmt.setLong(8, ts);
                    pstmt.setLong(9, seed);

                    pstmt.setLong(10, Double.valueOf(10 * Math.random()).intValue());
                    pstmt.setLong(11, Double.valueOf(10 * Math.random()).intValue());
                    pstmt.setLong(12, Double.valueOf(10 * Math.random()).intValue());
                    pstmt.setLong(13, Double.valueOf(10 * Math.random()).intValue());
                    pstmt.setLong(14, Double.valueOf(10 * Math.random()).intValue());
                    pstmt.setLong(15, Double.valueOf(10 * Math.random()).intValue());

                    pstmt.setLong(16, ts);
                    pstmt.setString(17, "usersegment" + seed % 10000);
                    pstmt.addBatch();
                    if ((seed % COMMIT_BATCH_SIZE) == 0){
                        pstmt.executeBatch();
                        conn.commit();
                        long end_timestamp = new java.util.Date().getTime();
                        synchronized (PhoenixClientThread.class) {
                            long time_taken = end_timestamp - start_timestamp;
                            totalTime = totalTime + time_taken;
                            System.out.print("time_taken = " + time_taken);
                            System.out.print(" threadNumber = " + threadNumber);
                            System.out.print(" totalTime = " + totalTime);
                            System.out.print(" averageTime = " + totalTime / count);
                            System.out.print(" iteration = " + seed);
                            System.out.print(" time_taken = " + time_taken);
                            System.out.println("");
                        }
                    }
                }

            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                pstmt.close();
                conn.close();
            }
        }
    }
}
