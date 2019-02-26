package com.packt.mapreduce.reducejoin;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
public class UserMapper extends Mapper<Object, Text, Text, Text> {
    private Text outputkey = new Text();
    private Text outputvalue = new Text();
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] userRecord = value.toString().split(",");
        String userId = userRecord[0];
        outputkey.set(userId);
        outputvalue.set("X" + value.toString());
        context.write(outputkey, outputvalue);
    }
}
