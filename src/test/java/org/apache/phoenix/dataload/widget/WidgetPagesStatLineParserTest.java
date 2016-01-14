package org.apache.phoenix.dataload.widget;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by thangar on 5/30/15.
 */
public class WidgetPagesStatLineParserTest {

    public static String line = "977|978|1393476903000|flex-lpmodelherofeature|aboutspecials|gmps-johnl-sullivan|\"\"|2wd light duty chassis-cab trucks&2wd standard pickup trucks&4-door mid-size passenger car&buick&chevrolet&corvette&gmc&gmlslp-buick&gmlslp-chevrolet&gmps-johnl-sullivan&gmps-performance-ca&gmps-reliable-pontiac&gmps-walser-roseville&gm-t1-chevrolet&gm-t1-gmc|desktop|2457188|1|2|3|958|25238|12367";
    @Test
    public void testParsePagesSegmentWidgetSingleLine() throws IOException {
        WidgetPagesStatLineParser parser = new WidgetPagesStatLineParser();
        WidgetPagesStat widgetPagesStat = parser.parse(line);

        Assert.assertNotNull(widgetPagesStat.getWebId());
        Assert.assertNotNull(widgetPagesStat.getWidgetContext());

        Assert.assertEquals("gmps-johnl-sullivan", widgetPagesStat.getWebId());
        Assert.assertEquals("aboutspecials", widgetPagesStat.getWebPageLabel());
        Assert.assertEquals("desktop", widgetPagesStat.getDeviceType());
        Assert.assertEquals(978, widgetPagesStat.getWidgetInstanceId());
        Assert.assertEquals(1393476903000L, widgetPagesStat.getWidgetVersion());
        Assert.assertEquals("flex-lpmodelherofeature", widgetPagesStat.getWidgetType());
        Assert.assertEquals("\"\"", widgetPagesStat.getWidgetContext());

        Assert.assertEquals(2, widgetPagesStat.getTotalClicks());
        Assert.assertEquals(3, widgetPagesStat.getTotalClickViews());
        Assert.assertEquals(958, widgetPagesStat.getTotalHoverTime());
        Assert.assertEquals(25238, widgetPagesStat.getTotalTimeOnPage());
        Assert.assertEquals(12367, widgetPagesStat.getTotalViewableTime());
    }
}
