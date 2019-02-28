package com.packt.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
public class WordcountMapper extends Mapper<LongWritable, Text, Text,
        IntWritable> {
    public static final IntWritable ONE = new IntWritable(1);
    @Override
    protected void map(LongWritable offset, Text line, Context context)
            throws IOException, InterruptedException {
        String[] result = line.toString().split(" ");
        for (String word : result) {
            context.write(new Text(word), ONE);
        }
    }
}