package com.packt.mapreduce;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class DemoMapper extends Mapper<LongWritable, Text, Text,
        IntWritable> {
    @Override
    protected void setup(Context context) throws IOException,
            InterruptedException {
        super.setup(context);
    }
    @Override
    protected void map(LongWritable key, Text value, Context
            context) throws IOException, InterruptedException {
        //Record Processing Logic Here
    }
    @Override
    protected void cleanup(Context context) throws IOException,
            InterruptedException {
        super.cleanup(context);
    }
}
