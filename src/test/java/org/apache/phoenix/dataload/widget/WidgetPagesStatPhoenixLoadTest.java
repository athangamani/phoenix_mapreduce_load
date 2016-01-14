package org.apache.phoenix.dataload.widget;

import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.junit.Test;

import java.sql.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collection;
import java.util.LinkedList;

/**
 * Created by thangar on 11/16/15.
 */
public class WidgetPagesStatPhoenixLoadTest {


    public static final String TIMESTAMP_FORMAT = "yyyyMMdd";
    public static final int TOTAL_NUMBER_OF_USERS = 5;
    public static final int TOTAL_ITERATIONS_PER_USER = 50;

    @Test
    public void testLoad() throws SQLException, InterruptedException {
        Collection<Thread> threadBagReference = new LinkedList<Thread>();
        for (int i = 0; i < TOTAL_NUMBER_OF_USERS; i++) {
            System.out.println("kicking off thread number = " + i);
            Thread executionThread = new Thread(new PhoenixClientThread(i));
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

        public PhoenixClientThread(int threadNumber) {
            this.threadNumber = threadNumber;
        }

        public void run() {
            try {
                DriverManager.registerDriver(PhoenixDriver.INSTANCE);
                Connection connection = DriverManager.getConnection("jdbc:phoenix:tukpdmdlake03.tuk.cobaltgroup.com,tukpdmdlake04.tuk.cobaltgroup.com,tukpdmdlake05.tuk.cobaltgroup.com:2181:/hbase-unsecure");
                //Connection connection = DriverManager.getConnection("jdbc:phoenix:master01.preprod.datalake.cdk.com,master02.preprod.datalake.cdk.com,master03.preprod.datalake.cdk.com:2181:/hbase-unsecure");
                int iteration = 1;
                while (iteration <= TOTAL_ITERATIONS_PER_USER) {
                    String webid = retrieveWebId();
                    retrieveData(connection, webid, iteration);
                    iteration ++;
                }
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }

        private void retrieveData(Connection connection, String webid, int iteration) throws SQLException, ParseException {
            DateFormat dateFormat = new SimpleDateFormat(TIMESTAMP_FORMAT);
            Calendar calReturn = Calendar.getInstance();
            calReturn.add(Calendar.DATE, -30);
            String earlierDate = dateFormat.format(calReturn.getTime());
            long earlierDateTimestamp = dateFormat.parse(earlierDate).getTime();
            long start_timestamp = new java.util.Date().getTime();
            String query = "SELECT SUM(total_clicks) ,\n" +
                    "SUM(total_click_views), \n" +
                    "SUM(total_time_on_page_ms), \n" +
                    "SUM(total_viewable_time_ms), \n" +
                    "SUM(total_hover_time_ms), \n" +
                    "SUM(view_count), \n" +
                    "widget_instance_id, \n" +
                    "widget_type, \n" +
                    "widget_version, \n" +
                    "widget_context \n" +
                    "from WIDGET_PAGES_STAT \n" +
                    "where web_id =  '" + webid + "' \n" +
                    "and web_page_label = "  + "'homepage' \n" +
                    "group by widget_instance_id, widget_type, widget_version, widget_context";

            //System.out.println("query = " + query);

            PreparedStatement statement = connection.prepareStatement(query);
            ResultSet resultSet = statement.executeQuery();

            int count = 0;
            while (resultSet.next()) {
                count++;
                PhoenixResultSet phoenixResultSet = (PhoenixResultSet) resultSet;
                //System.out.print('total_clicks = ' + phoenixResultSet.getLong(1));
                //System.out.print('total_click_views = ' + phoenixResultSet.getLong(2));
                //System.out.print('total_time_on_page_ms = ' + phoenixResultSet.getLong(3));
                //System.out.print('total_viewable_time_ms = ' + phoenixResultSet.getLong(4));
                //System.out.print('total_hover_time_ms = ' + phoenixResultSet.getLong(5));
                //System.out.print('view_count = ' + phoenixResultSet.getLong(6));
                //System.out.print('widget_instance_id = ' + phoenixResultSet.getLong(7));
                //System.out.print('widget_type = ' + phoenixResultSet.getString(8));
                //System.out.print('widget_version = ' + phoenixResultSet.getString(9));
                //System.out.print("widget_context = " + phoenixResultSet.getString(10));
            }
            long end_timestamp = new java.util.Date().getTime();
            synchronized (PhoenixClientThread.class) {
                long time_taken = end_timestamp - start_timestamp;
                totalTime = totalTime + time_taken;
                System.out.print("time_taken = " + time_taken);
                System.out.print(" total Number of records = " + count);
                System.out.print(" threadNumber = " + threadNumber);
                System.out.print(" totalTime = " + totalTime);
                System.out.print(" averageTime = " + totalTime / iteration);
                System.out.print(" iteration = " + iteration);
                System.out.print(" time_taken = " + time_taken);
                System.out.println("");
            }
            statement.close();
        }

        private String retrieveWebId() {
            double random = Math.random();
            int index = (int) (webIds.length * random);
            return webIds[index];
        }

        String[] webIds = {
                "motp-sewell-portal", "gmps-laura", "gmps-dan-cummins", "gmps-classic", "gmps-karl-chev",
                "gmps-courtesy-az", "gmps-kendall-fl", "gmps-sundance-chev", "gmps-stingray", "gmps-henna-austin",
                "gmps-thorobred", "gmps-sapaugh", "gmps-reliable-tx", "gmps-jimbutler", "gmps-jack", "gmps-good-renton",
                "gmps-city-nc", "gmps-mac-mulkin", "gmps-bachman", "gmps-christopher-chevy", "gmps-covert-chev",
                "gmps-buff-whelan", "gmps-dominion-sp", "motp-donmoore-portal", "gmps-best-chevrolet", "gmps-covert-city",
                "gmps-rickhendrick-chev-nc", "gmps-sugarland", "gmps-kerbeck-subaru", "gmps-moritz-chevrolet",
                "gmps-bradshawauto", "gmps-hewlett", "gmps-sinclair-mo", "gmps-mayse-earnhardt", "gmps-terrylabonte",
                "gmps-findlay", "gmps-serpentini-strongsville", "gmps-loehmann", "gmps-johnl-sullivan",
                "gmps-johnnylondoff", "gmps-jeff-gordon", "gmps-bob-johnson", "gmps-jim-glover", "gmps-jba-chev",
                "gmps-miller-mn", "gmps-capitol-chevrolet", "gmps-suburban-mn", "gmps-everett-ar", "gmps-george-matick",
                "motp-phil-portal", "gmps-shaheen", "gmps-moran-chevy", "gmps-win", "gmps-rydell-auto-ca",
                "gmps-richard-ct", "gmps-gateway-tx", "gmps-dimmitt-chevy", "gmps-denny", "gmps-sunset-chev",
                "gmps-weber-chevrolet", "gmps-bobbrown-chev", "gmps-jerry-chevy", "gmps-service-la", "gmps-everett",
                "gmps-northstar-ny", "gmps-criswell", "gmps-pete-moore", "gmps-peoria", "gmps-tom-benson",
                "gmps-voss", "gmps-huffines-chev", "gmps-elco-chevy", "gmps-diver", "gmps-northstar",
                "gmps-rosedale", "motp-kia-east", "gmps-sutliff", "gmps-ferman", "gmps-rapid", "gmps-woody-folsom",
                "gmps-king-longmont", "gmps-ostrom", "gmps-young", "gmps-bergeyschev", "gmps-orielly", "gmps-green-il",
                "gmps-c-o-wv", "gmps-hubler", "gmps-paddock", "gmps-love", "gmps-chevyland", "gmps-rogers-usry",
                "gmps-hurd-cars", "gmps-country-chevrolet", "gmps-vandevere-chev", "gmps-hh-chev", "gmps-sands-chev",
                "gmps-gateway", "motp-imperial-cdj", "gmps-freeman"
        };
    }
}
