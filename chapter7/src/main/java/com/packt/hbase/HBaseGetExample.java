package com.packt.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;
public class HBaseGetExample {
    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        HTable table = new HTable(conf, "testtable");
        Get get = new Get(Bytes.toBytes("samplerow"));
        get.addColumn(Bytes.toBytes("colfamily1"), Bytes.toBytes("col1"));
        Result result = table.get(get);
        byte[] val = result.getValue(Bytes.toBytes("colfamily1"),
                Bytes.toBytes("col2"));
        System.out.println("Value: " + Bytes.toString(val));
    }
}