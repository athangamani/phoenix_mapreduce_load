package org.apache.phoenix.dataload.widget;

import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.PrintWriter;

/**
 * Created by thangar on 1/14/16.
 */
public class WidgetPagesStatDataCreatorTest {

    @Test
    public void testGenerateData() throws FileNotFoundException {

        int totalNumberOfRecords = 10000000;
        PrintWriter writer = new PrintWriter("/Users/thangar/phoenix_logs/data-1.txt");

        for (int seed=0;seed<totalNumberOfRecords;seed++) {
            StringBuilder sb = new StringBuilder();
            sb.append(seed); //rownumber
            sb.append("|");
            sb.append(seed % 10000); //widgetinstance
            sb.append("|");
            sb.append(seed % 10000); //widgetversion
            sb.append("|");
            sb.append("widgetype" + seed % 10000); //widgettype
            sb.append("|");
            sb.append("pagelabel" + seed % 10000); //webpagelabel
            sb.append("|");
            sb.append("webid" + seed % 10000); //webid
            sb.append("|");
            sb.append("widgetcontext" + seed % 10000); //widgetcontext
            sb.append("|");
            sb.append("usersegment" + seed % 10000); //usersegment
            sb.append("|");
            sb.append("deviceid" + seed % 10000); //devicetype
            sb.append("|");
            long dimdateKey = System.currentTimeMillis();
            sb.append(dimdateKey); //dimdatekey
            sb.append("|");
            sb.append(Double.valueOf(100 * Math.random()).intValue());
            sb.append("|");
            sb.append(Double.valueOf(100 * Math.random()).intValue());
            sb.append("|");
            sb.append(Double.valueOf(100 * Math.random()).intValue());
            sb.append("|");
            sb.append(Double.valueOf(100 * Math.random()).intValue());
            sb.append("|");
            sb.append(Double.valueOf(100 * Math.random()).intValue());
            sb.append("|");
            sb.append(Double.valueOf(100 * Math.random()).intValue());
            sb.append("\n");
            writer.write(sb.toString());
        }
        writer.close();
    }
}
