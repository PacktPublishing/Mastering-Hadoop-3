package com.packt.mapreduce.compositejoin;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.join.CompositeInputFormat;
public class CompositeJoinExampleDriver {
    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf("CompositeJoin");
        conf.setJarByClass(CompositeJoinExampleDriver.class);
        if (args.length < 2) {
            System.out.println("Jar requires 4 paramaters : \""
                            + conf.getJar()
                            + " input_path1 input_path2 output_path jointype[outer or inner] ");
            System.exit(1);
        }

        conf.setMapperClass(CompositeJoinMapper.class);
        conf.setNumReduceTasks(0);
        conf.setInputFormat(CompositeInputFormat.class);
        conf.set("mapred.join.expr", CompositeInputFormat.compose(args[3],
                KeyValueTextInputFormat.class, new Path(args[0]), new
                        Path(args[1])));
        TextOutputFormat.setOutputPath(conf,new Path(args[2]));
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        RunningJob job = JobClient.runJob(conf);
        System.exit(job.isSuccessful() ? 0 : 1);
    }
}
