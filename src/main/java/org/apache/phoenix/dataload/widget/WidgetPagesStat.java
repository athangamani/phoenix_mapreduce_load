package org.apache.phoenix.dataload.widget;

/**
 * Created by thangar on 9/3/15.
 */
public final class WidgetPagesStat {

    private String webId;
    private String webPageLabel;
    private long widgetInstanceId;
    private String widgetType;
    private long widgetVersion;
    private String widgetContext;
    private long totalClicks;
    private long totalClickViews;
    private long totalTimeOnPage;
    private long totalViewableTime;
    private long totalHoverTime;
    private String deviceType;
    private long viewCount;
    private long viewDateTimestamp;
    private String userSegments;
    private long dimDateKey;
    private long rowNumber;

    public String getWebId() {
        return webId;
    }

    public void setWebId(String webId) {
        this.webId = webId;
    }

    public String getWebPageLabel() {
        return webPageLabel;
    }

    public void setWebPageLabel(String webPageLabel) {
        this.webPageLabel = webPageLabel;
    }

    public long getWidgetInstanceId() {
        return widgetInstanceId;
    }

    public void setWidgetInstanceId(long widgetInstanceId) {
        this.widgetInstanceId = widgetInstanceId;
    }

    public String getWidgetType() {
        return widgetType;
    }

    public void setWidgetType(String widgetType) {
        this.widgetType = widgetType;
    }

    public long getWidgetVersion() {
        return widgetVersion;
    }

    public void setWidgetVersion(long widgetVersion) {
        this.widgetVersion = widgetVersion;
    }

    public String getWidgetContext() {
        return widgetContext;
    }

    public void setWidgetContext(String widgetContext) {
        this.widgetContext = widgetContext;
    }

    public long getTotalClicks() {
        return totalClicks;
    }

    public void setTotalClicks(long totalClicks) {
        this.totalClicks = totalClicks;
    }

    public long getTotalClickViews() {
        return totalClickViews;
    }

    public void setTotalClickViews(long totalClickViews) {
        this.totalClickViews = totalClickViews;
    }

    public long getTotalTimeOnPage() {
        return totalTimeOnPage;
    }

    public void setTotalTimeOnPage(long totalTimeOnPage) {
        this.totalTimeOnPage = totalTimeOnPage;
    }

    public long getTotalViewableTime() {
        return totalViewableTime;
    }

    public void setTotalViewableTime(long totalViewableTime) {
        this.totalViewableTime = totalViewableTime;
    }

    public long getTotalHoverTime() {
        return totalHoverTime;
    }

    public void setTotalHoverTime(long totalHoverTime) {
        this.totalHoverTime = totalHoverTime;
    }

    public String getDeviceType() {
        return deviceType;
    }

    public void setDeviceType(String deviceType) {
        this.deviceType = deviceType;
    }

    public long getViewCount() {
        return viewCount;
    }

    public void setViewCount(long viewCount) {
        this.viewCount = viewCount;
    }

    public long getViewDateTimestamp() {
        return viewDateTimestamp;
    }

    public void setViewDateTimestamp(long viewDateTimestamp) {
        this.viewDateTimestamp = viewDateTimestamp;
    }

    public String getUserSegments() {
        return userSegments;
    }

    public void setUserSegments(String userSegments) {
        this.userSegments = userSegments;
    }

    public long getDimDateKey() {
        return dimDateKey;
    }

    public void setDimDateKey(long dimDateKey) {
        this.dimDateKey = dimDateKey;
    }

    public long getRowNumber() {
        return rowNumber;
    }

    public void setRowNumber(long rowNumber) {
        this.rowNumber = rowNumber;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        WidgetPagesStat that = (WidgetPagesStat) o;

        if (dimDateKey != that.dimDateKey) return false;
        if (rowNumber != that.rowNumber) return false;
        if (totalTimeOnPage != that.totalTimeOnPage) return false;
        if (totalClickViews != that.totalClickViews) return false;
        if (totalClicks != that.totalClicks) return false;
        if (totalHoverTime != that.totalHoverTime) return false;
        if (totalViewableTime != that.totalViewableTime) return false;
        if (viewCount != that.viewCount) return false;
        if (viewDateTimestamp != that.viewDateTimestamp) return false;
        if (widgetInstanceId != that.widgetInstanceId) return false;
        if (widgetVersion != that.widgetVersion) return false;
        if (deviceType != null ? !deviceType.equals(that.deviceType) : that.deviceType != null) return false;
        if (userSegments != null ? !userSegments.equals(that.userSegments) : that.userSegments != null) return false;
        if (webId != null ? !webId.equals(that.webId) : that.webId != null) return false;
        if (webPageLabel != null ? !webPageLabel.equals(that.webPageLabel) : that.webPageLabel != null) return false;
        if (widgetContext != null ? !widgetContext.equals(that.widgetContext) : that.widgetContext != null)
            return false;
        if (widgetType != null ? !widgetType.equals(that.widgetType) : that.widgetType != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = webId != null ? webId.hashCode() : 0;
        result = 31 * result + (webPageLabel != null ? webPageLabel.hashCode() : 0);
        result = 31 * result + (int) (widgetInstanceId ^ (widgetInstanceId >>> 32));
        result = 31 * result + (widgetType != null ? widgetType.hashCode() : 0);
        result = 31 * result + (int) (widgetVersion ^ (widgetVersion >>> 32));
        result = 31 * result + (widgetContext != null ? widgetContext.hashCode() : 0);
        result = 31 * result + (int) (totalClicks ^ (totalClicks >>> 32));
        result = 31 * result + (int) (totalClickViews ^ (totalClickViews >>> 32));
        result = 31 * result + (int) (totalTimeOnPage ^ (totalTimeOnPage >>> 32));
        result = 31 * result + (int) (totalViewableTime ^ (totalViewableTime >>> 32));
        result = 31 * result + (int) (totalHoverTime ^ (totalHoverTime >>> 32));
        result = 31 * result + (deviceType != null ? deviceType.hashCode() : 0);
        result = 31 * result + (int) (viewCount ^ (viewCount >>> 32));
        result = 31 * result + (int) (viewDateTimestamp ^ (viewDateTimestamp >>> 32));
        result = 31 * result + (userSegments != null ? userSegments.hashCode() : 0);
        result = 31 * result + (int) (dimDateKey ^ (dimDateKey >>> 32));
        result = 31 * result + (int) (rowNumber ^ (rowNumber >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "WidgetPhoenix{" +
                "webId='" + webId + '\'' +
                ", webPageLabel='" + webPageLabel + '\'' +
                ", widgetInstanceId=" + widgetInstanceId +
                ", widgetType='" + widgetType + '\'' +
                ", widgetVersion=" + widgetVersion +
                ", widgetContext='" + widgetContext + '\'' +
                ", totalClicks=" + totalClicks +
                ", totalClickViews=" + totalClickViews +
                ", totalTimeOnPage=" + totalTimeOnPage +
                ", totalViewableTime=" + totalViewableTime +
                ", totalHoverTime=" + totalHoverTime +
                ", deviceType='" + deviceType + '\'' +
                ", viewCount=" + viewCount +
                ", viewDateTimestamp=" + viewDateTimestamp +
                ", userSegments='" + userSegments + '\'' +
                ", dimDateKey=" + dimDateKey +
                ", rowNumber=" + rowNumber +
                '}';
    }
}
