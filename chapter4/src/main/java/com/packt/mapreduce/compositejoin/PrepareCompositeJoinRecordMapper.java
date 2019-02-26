package com.packt.mapreduce.compositejoin;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.List;
public class PrepareCompositeJoinRecordMapper extends Mapper<LongWritable,
        Text, Text, Text> {
    private int indexOfKey=0;
    private Splitter splitter;
    private Joiner joiner;
    private Text joinKey = new Text();
    String separator=",";
    @Override
    protected void setup(Context context) throws IOException,
            InterruptedException {
        splitter = Splitter.on(separator);
        joiner = Joiner.on(separator);
    }
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        Iterable<String> recordColumns = splitter.split(value.toString());
        joinKey.set(Iterables.get(recordColumns, indexOfKey));
        if(indexOfKey != 0){
            value.set(getRecordInCompositeJoinFormat(recordColumns,
                    indexOfKey));
        }
        context.write(joinKey,value);
    }
    private String getRecordInCompositeJoinFormat(Iterable<String> value,
                                                  int index){
        List<String> temp = Lists.newArrayList(value);
        String originalFirst = temp.get(0);
        String newFirst = temp.get(index);
        temp.set(0,newFirst);
        temp.set(index,originalFirst);
        return joiner.join(temp);
    }
}


