package org.apache.phoenix.dataload.widget;


import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * Created by thangar on 5/30/15.
 */
public class WidgetPagesStatLineParser {

    public static final String FIELD_SEPERATOR = "|";
    public static final String TIMESTAMP_FORMAT = "yyyyMMdd";

    public WidgetPagesStat parse (String line) throws NumberFormatException{
        WidgetPagesStat widgetPagesStat = null;

        String[] values = split(line, FIELD_SEPERATOR);
        if (values.length == 16) {
            widgetPagesStat = new WidgetPagesStat();

                widgetPagesStat.setRowNumber(new Long(values[0]));
                widgetPagesStat.setWidgetInstanceId(new Long(values[1]));
                widgetPagesStat.setWidgetVersion(new Long(values[2]));
                widgetPagesStat.setWidgetType(values[3]);

                widgetPagesStat.setWebPageLabel(values[4]);
                widgetPagesStat.setWebId(values[5]);
                widgetPagesStat.setWidgetContext(values[6]);
                widgetPagesStat.setUserSegments(values[7]);
                widgetPagesStat.setDeviceType(values[8]);
                widgetPagesStat.setDimDateKey(new Long(values[9]));

                widgetPagesStat.setViewCount(new Long(values[10]));
                widgetPagesStat.setTotalClicks(new Long(values[11]));
                widgetPagesStat.setTotalClickViews(new Long(values[12]));
                widgetPagesStat.setTotalHoverTime(new Long(values[13]));
                widgetPagesStat.setTotalTimeOnPage(new Long(values[14]));
                widgetPagesStat.setTotalViewableTime(new Long(values[15]));
        }
        return widgetPagesStat;
    }

    private String[] split(String line, String separtor) {
        return line.split("\\" + separtor, -1);
    }

    public static Long parseDateString(String dateString) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(TIMESTAMP_FORMAT);
        try {
            return simpleDateFormat.parse(dateString).getTime();
        } catch (ParseException e) {
            //if we cannot parse correctly, simply return null no need to print the stack trace at high volumes
            //e.printStackTrace();
        }
        return null;
    }
}
