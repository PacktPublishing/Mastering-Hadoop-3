package com.packt.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
public class DemoReducer extends Reducer<Text, IntWritable, Text,
        IntWritable> {
    @Override
    protected void setup(Context context) throws IOException,
            InterruptedException {
        super.setup(context);
    }
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values,
                          Context context) throws IOException, InterruptedException {
        super.reduce(key, values, context);
    }
    @Override
    protected void cleanup(Context context) throws IOException,
            InterruptedException {
        super.cleanup(context);
    }
}