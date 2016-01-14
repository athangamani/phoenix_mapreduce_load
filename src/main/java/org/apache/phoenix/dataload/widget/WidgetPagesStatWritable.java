package org.apache.phoenix.dataload.widget;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by thangar on 9/3/15.
 */
public class WidgetPagesStatWritable implements DBWritable, Writable {

    private WidgetPagesStat widgetPagesStat;

    public void readFields(DataInput input) throws IOException {
        widgetPagesStat.setWebId(input.readLine());
        widgetPagesStat.setWebPageLabel(input.readLine());

        widgetPagesStat.setWidgetInstanceId(input.readLong());
        widgetPagesStat.setWidgetType(input.readLine());
        widgetPagesStat.setWidgetVersion(input.readLong());
        widgetPagesStat.setWidgetContext(input.readLine());

        widgetPagesStat.setTotalClicks(input.readLong());
        widgetPagesStat.setTotalClickViews(input.readLong());
        widgetPagesStat.setTotalTimeOnPage(input.readLong());
        widgetPagesStat.setTotalViewableTime(input.readLong());
        widgetPagesStat.setTotalHoverTime(input.readLong());

        widgetPagesStat.setDeviceType(input.readLine());

        widgetPagesStat.setViewCount(input.readLong());
        widgetPagesStat.setViewDateTimestamp(input.readLong());
        widgetPagesStat.setViewDate(input.readLine());
        widgetPagesStat.setUserSegments(input.readLine());

        widgetPagesStat.setDimDateKey(input.readLong());
        widgetPagesStat.setRowNumber(input.readLong());
    }

    public void write(DataOutput output) throws IOException {
        output.writeBytes(widgetPagesStat.getWebId());
        output.writeBytes(widgetPagesStat.getWebPageLabel());

        output.writeLong(widgetPagesStat.getWidgetInstanceId());
        output.writeBytes(widgetPagesStat.getWidgetType());
        output.writeLong(widgetPagesStat.getWidgetVersion());
        output.writeBytes(widgetPagesStat.getWidgetContext());

        output.writeLong(widgetPagesStat.getTotalClicks());
        output.writeLong(widgetPagesStat.getTotalClickViews());
        output.writeLong(widgetPagesStat.getTotalHoverTime());
        output.writeLong(widgetPagesStat.getTotalViewableTime());
        output.writeLong(widgetPagesStat.getTotalTimeOnPage());

        output.writeBytes(widgetPagesStat.getDeviceType());

        output.writeLong(widgetPagesStat.getViewCount());
        output.writeLong(widgetPagesStat.getViewDateTimestamp());
        output.writeBytes(widgetPagesStat.getViewDate());
        output.writeBytes(widgetPagesStat.getUserSegments());

        output.writeLong(widgetPagesStat.getDimDateKey());
        output.writeLong(widgetPagesStat.getRowNumber());
    }

    public void readFields(ResultSet rs) throws SQLException {
        widgetPagesStat.setWebId(rs.getString("WEB_ID"));
        widgetPagesStat.setWebPageLabel(rs.getString("WEB_PAGE_LABEL"));
        widgetPagesStat.setWidgetInstanceId(rs.getLong("WIDGET_INSTANCE_ID"));
        widgetPagesStat.setWidgetType(rs.getString("WIDGET_TYPE"));
        widgetPagesStat.setWidgetVersion(rs.getLong("WIDGET_VERSION"));
        widgetPagesStat.setWidgetContext(rs.getString("WIDGET_CONTEXT"));

        widgetPagesStat.setTotalClicks(rs.getLong("TOTAL_CLICKS"));
        widgetPagesStat.setTotalClickViews(rs.getLong("TOTAL_CLICK_VIEWS"));
        widgetPagesStat.setTotalTimeOnPage(rs.getLong("TOTAL_TIME_ON_PAGE_MS"));
        widgetPagesStat.setTotalViewableTime(rs.getLong("TOTAL_VIEWABLE_TIME_MS"));
        widgetPagesStat.setTotalHoverTime(rs.getLong("TOTAL_HOVER_TIME_MS"));
        widgetPagesStat.setDeviceType(rs.getString("DEVICE_TYPE"));
        widgetPagesStat.setViewCount(rs.getLong("VIEW_COUNT"));
        widgetPagesStat.setViewDateTimestamp(rs.getLong("VIEW_DATE_TIMESTAMP"));
        widgetPagesStat.setViewDate(rs.getString("VIEW_DATE"));
        widgetPagesStat.setUserSegments(rs.getString("USER_SEGMENT"));

        widgetPagesStat.setDimDateKey(rs.getLong("DIM_DATE_KEY"));
        widgetPagesStat.setRowNumber(rs.getLong("ROW_NUMBER"));
    }

    public void write(PreparedStatement pstmt) throws SQLException {
        pstmt.setString(1, widgetPagesStat.getWebId());
        pstmt.setString(2, widgetPagesStat.getWebPageLabel());
        pstmt.setString(3, widgetPagesStat.getDeviceType());

        pstmt.setLong(4, widgetPagesStat.getWidgetInstanceId());
        pstmt.setString(5, widgetPagesStat.getWidgetType());
        pstmt.setLong(6, widgetPagesStat.getWidgetVersion());
        pstmt.setString(7, widgetPagesStat.getWidgetContext());

        pstmt.setLong(8, widgetPagesStat.getTotalClicks());
        pstmt.setLong(9, widgetPagesStat.getTotalClickViews());
        pstmt.setLong(10, widgetPagesStat.getTotalHoverTime());
        pstmt.setLong(11, widgetPagesStat.getTotalTimeOnPage());
        pstmt.setLong(12, widgetPagesStat.getTotalViewableTime());
        pstmt.setLong(13, widgetPagesStat.getViewCount());

        pstmt.setString(14, widgetPagesStat.getUserSegments());
        pstmt.setLong(15, widgetPagesStat.getDimDateKey());
        pstmt.setString(16, widgetPagesStat.getViewDate());
        pstmt.setLong(17, widgetPagesStat.getViewDateTimestamp());

        pstmt.setLong(18, widgetPagesStat.getRowNumber());
    }

    public WidgetPagesStat getWidgetPagesStat() {
        return widgetPagesStat;
    }

    public void setWidgetPagesStat(WidgetPagesStat widgetPagesStat) {
        this.widgetPagesStat = widgetPagesStat;
    }
}