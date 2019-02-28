package com.packt.mapreduce.reducejoin;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
public class PurchaseReportMapper extends Mapper<Object, Text, Text, Text>{
    private Text outputkey = new Text();
    private Text outputvalue = new Text();
    public void map(Object key, Text value, Mapper.Context context)
            throws IOException, InterruptedException {
        String[] purchaseRecord = value.toString().split(",");
        String userId = purchaseRecord[1];
        outputkey.set(userId);
        outputvalue.set("Y" + value.toString());
        context.write(outputkey, outputvalue);
    }
}