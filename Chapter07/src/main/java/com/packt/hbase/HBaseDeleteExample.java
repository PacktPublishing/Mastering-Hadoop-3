package com.packt.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
public class HBaseDeleteExample {
    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        HTable table = new HTable(conf, "testtable");
        List<Delete> deletes = new ArrayList<Delete>();
        Delete delete1 = new Delete(Bytes.toBytes("samplerow"));
        delete1.setTimestamp(4);
        deletes.add(delete1);
        Delete delete2 = new Delete(Bytes.toBytes("row2"));
        delete2.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("col1"));
        delete2.addColumn(Bytes.toBytes("colfam2"), Bytes.toBytes("col3"),
                5);
        deletes.add(delete2);
        Delete delete3 = new Delete(Bytes.toBytes("row3"));
        delete3.addFamily(Bytes.toBytes("colfamily1"));
        delete3.addFamily(Bytes.toBytes("colfamily1"), 3);
        deletes.add(delete3);
        table.delete(deletes);
    }
}