package com.packt.hadoopdesign;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
public class PacktSequenceFileWriter {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        int i =0;
        try {
            FileSystem fs = FileSystem.get(conf);
            File file = new File("/home/packt/test.txt");
            Path outFile = new Path(args[0]);
            IntWritable key = new IntWritable();
            Text value = new Text();
            SequenceFile.Writer sequneceWriter = null;
            try {
                // creating sequneceQriter
                sequneceWriter = SequenceFile.createWriter(fs, conf, outFile,
                        key.getClass(), value.getClass());
                for (String line : FileUtils.readLines(file)) {
                    key.set(i++);
                    value.set(line);
                    sequneceWriter.append(key, value);
                }
            }finally {
                if(sequneceWriter != null) {
                    sequneceWriter.close();
                }
            }

        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
