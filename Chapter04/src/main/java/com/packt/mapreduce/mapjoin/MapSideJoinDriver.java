package com.packt.mapreduce.mapjoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.util.Map;
public class MapSideJoinDriver {
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), (Tool) new
                MapSideJoinDriver(), args);
        System.exit(res);
    }
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "map join");
        job.setJarByClass(MapSideJoinDriver.class);
        if (args.length < 3) {
            System.out.println("Jar requires 3 paramaters : \""
                    + job.getJar()
                    + " input_path output_path distributedcachefile");
            return 1;
        }


        job.addCacheFile(new Path(args[2]).toUri());
        job.setMapperClass(UserPurchaseMapSideJoinMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        Path filePath = new Path(args[0]);
        FileInputFormat.setInputPaths(job, filePath);
        Path outputPath = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outputPath);
        job.waitForCompletion(true);
        return 0;
    }
}
