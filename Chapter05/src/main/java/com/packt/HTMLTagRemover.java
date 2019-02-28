package com.packt;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.jsoup.Jsoup;

public class HTMLTagRemover extends UDF {
    public String evaluate(String column) {
        if (column == null)
            return null;
        return Jsoup.parse(column).text();
    }
}