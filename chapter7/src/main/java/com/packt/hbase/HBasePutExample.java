package com.packt.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;
public class HBasePutExample {


    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        HTable table = new HTable(conf, "testtable");
        Put recordPuter = new Put(Bytes.toBytes("samplerow"));
        recordPuter.addColumn(Bytes.toBytes("colfamily1"),
                Bytes.toBytes("col1"),
                Bytes.toBytes("val1"));
        recordPuter.addColumn(Bytes.toBytes("colfamily1"),
                Bytes.toBytes("col2"),
                Bytes.toBytes("val2"));
        table.put(recordPuter);
    }
}