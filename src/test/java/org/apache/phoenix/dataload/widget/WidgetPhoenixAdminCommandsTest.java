package org.apache.phoenix.dataload.widget;

import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDriver;

import java.sql.*;

/**
 * Created by thangar
 */
public class WidgetPhoenixAdminCommandsTest {

    public static final String TIMESTAMP_FORMAT = "yyyyMMdd";

    public static void main(String[] args) throws SQLException {
        DriverManager.registerDriver(PhoenixDriver.INSTANCE);
        //Connection connection = DriverManager.getConnection("jdbc:phoenix:tukpdmdlake03.tuk.cobaltgroup.com:/hbase-unsecure;CurrentSCN=100"); //
        Connection connection = DriverManager.getConnection("jdbc:phoenix:master01.preprod.datalake.cdk.com:2181:/hbase-unsecure;CurrentSCN=100"); //
        PhoenixConnection phoenixConnection = (PhoenixConnection)connection;
        System.out.println("phoenixConnection.getSCN() = " + phoenixConnection.getSCN());

        //dropTable(connection);
        createTable(connection);
        //alterTable(connection);
        //addData(connection);
        //readData(connection);
        //deleteData(connection);
        //truncateData(connection);
        //connection.close();
    }

    private static void deleteData(Connection connection) throws SQLException {
        Statement statement = connection.createStatement();
        statement.execute("delete from widget_pages_stat\n" +
                "where web_id = 'test-webid'");
        connection.commit();
    }

    private static void truncateData(Connection connection) throws SQLException {
        Statement statement = connection.createStatement();
        statement.execute("delete from WIDGET_PAGES_STATS \n");
        connection.commit();
    }

    private static void alterTable(Connection connection) throws SQLException{
        Statement statement = connection.createStatement();
        statement.execute("alter TABLE WIDGET_PAGES_STAT ADD user_segment VARCHAR");
        connection.commit();
    }

    private static void dropTable(Connection connection) throws SQLException {
        Statement statement = connection.createStatement();
        statement.execute("DROP TABLE WIDGET_PAGES_STAT");
        connection.commit();
    }

    private static void createTable(Connection connection) throws SQLException {
        Statement statement = connection.createStatement();
        statement.execute("\n" +
                "CREATE  TABLE WIDGET_PAGES_STAT  (\n" +
                "  web_id VARCHAR NOT NULL,\n" +
                "  web_page_label VARCHAR NOT NULL,\n" +
                "  device_type VARCHAR,\n" +
                "\n" +
                "\n" +
                "  widget_instance_id UNSIGNED_LONG NOT NULL,\n" +
                "  widget_type VARCHAR NOT NULL,\n" +
                "  widget_version UNSIGNED_LONG NOT NULL,\n" +
                "  widget_context VARCHAR NOT NULL,\n" +
                "\n" +
                "\n" +
                "  view_date_timestamp UNSIGNED_LONG NOT NULL,\n" +
                "  row_number UNSIGNED_LONG NOT NULL,\n" +
                "\n" +
                "\n" +
                "  total_clicks UNSIGNED_LONG,\n" +
                "  total_click_views UNSIGNED_LONG,\n" +
                "  total_time_on_page_ms UNSIGNED_LONG,\n" +
                "  total_viewable_time_ms UNSIGNED_LONG,\n" +
                "  total_hover_time_ms UNSIGNED_LONG,\n" +
                "  view_count UNSIGNED_LONG,\n" +
                "\n" +
                "  dim_date_key UNSIGNED_LONG,\n" +
                "  user_segment VARCHAR,\n" +
                "  CONSTRAINT pk PRIMARY KEY (web_id, web_page_label, device_type, widget_instance_id, widget_type, widget_version, widget_context, view_date_timestamp, row_number)\n" +
                ")\n" +
                "SALT_BUCKETS=32,\n" +
                "COMPRESSION='LZ4'"
                //"TTL=5184000"
        );
        connection.commit();
    }

    public static void addData(Connection connection) throws SQLException {
        connection.setAutoCommit(false);
        Statement sta = connection.createStatement();
        sta.execute("upsert into WIDGET_PAGES_STAT values('test-webid','homepage', 'mobile', 20150711, 'type1', 111, 'context1', 100000000, 2, 3, 4, 5, 6, 7, 8,  1000000000, '20150711', 'usersegment1')");
        connection.commit();
    }

    public static void readData(Connection connection) throws SQLException {
        connection.setAutoCommit(false);
        Statement sta = connection.createStatement();
        ResultSet resultSet = sta.executeQuery("select * from WIDGET_PAGES_STAT limit 100");
        while(resultSet.next()){
            System.out.println("web_id = " + resultSet.getString("WEB_ID"));
            System.out.println("WEB_PAGE_LABEL = " + resultSet.getString("WEB_PAGE_LABEL"));
            System.out.println("WIDGET_INSTANCE_ID = " + resultSet.getString("WIDGET_INSTANCE_ID"));
            System.out.println("WIDGET_TYPE = " + resultSet.getString("WIDGET_TYPE"));
            System.out.println("WIDGET_VERSION = " + resultSet.getString("WIDGET_VERSION"));
            System.out.println("WIDGET_CONTEXT = " + resultSet.getString("WIDGET_CONTEXT"));
            System.out.println("TOTAL_CLICKS = " + resultSet.getString("TOTAL_CLICKS"));
            System.out.println("TOTAL_CLICK_VIEWS = " + resultSet.getString("TOTAL_CLICK_VIEWS"));
            System.out.println("TOTAL_TIME_ON_PAGE_MS = " + resultSet.getString("TOTAL_TIME_ON_PAGE_MS"));
            System.out.println("TOTAL_VIEWABLE_TIME_MS = " + resultSet.getString("TOTAL_VIEWABLE_TIME_MS"));

            System.out.println("TOTAL_HOVER_TIME_MS = " + resultSet.getString("TOTAL_HOVER_TIME_MS"));
            System.out.println("DEVICE_TYPE = " + resultSet.getString("DEVICE_TYPE"));
            System.out.println("VIEW_COUNT = " + resultSet.getString("VIEW_COUNT"));
            System.out.println("VIEW_DATE_TIMESTAMP = " + resultSet.getString("VIEW_DATE_TIMESTAMP"));

            System.out.println("USER_SEGMENT = " + resultSet.getString("USER_SEGMENT"));
            System.out.println("DIM_DATE_KEY = " + resultSet.getString("DIM_DATE_KEY"));
            System.out.println("ROW_NUMBER = " + resultSet.getString("ROW_NUMBER"));

            System.out.println("----------------------------------------------------");
        }
        connection.commit();
    }
}