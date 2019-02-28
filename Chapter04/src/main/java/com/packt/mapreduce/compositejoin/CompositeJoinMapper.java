package com.packt.mapreduce.compositejoin;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.join.TupleWritable;
import java.io.IOException;
public class CompositeJoinMapper extends MapReduceBase implements
        Mapper<Text, TupleWritable, Text, Text> {
    public void map(Text text, TupleWritable value, OutputCollector<Text,
            Text> outputCollector, Reporter reporter) throws IOException {
        outputCollector.collect((Text) value.get(0), (Text) value.get(1));
    }
}