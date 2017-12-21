package com.kakao.hbase.tools;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Util {
    static String SEPARATOR =   "==========================================================" +
                                "==========================================================";
    static String SEPARATOR2 =  "----------------------------------------------------------" +
                                "----------------------------------------------------------";

    static void println (String s) {
        System.out.println(s);
    }

    public String getCurrentTs() {
        DateFormat dateFormat = new SimpleDateFormat("yy/MM/dd HH:mm:ss");
        Date date = new Date();
        return dateFormat.format(date);
    }

    public String getLastString(String line, String delemiter) {
        String[] temp = line.split(delemiter);
        return temp[temp.length - 1];
    }
}
