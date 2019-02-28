package com.packt.mapreduce.minmax;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
public class MinMaxMapper extends
        Mapper<LongWritable, Text, Text, PlayerDetail> {
    private PlayerDetail playerDetail = new PlayerDetail();
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] player = value.toString().split(",");
        playerDetail.setPlayerName(new Text(player[0]));
        playerDetail.setScore(new
                IntWritable(Integer.parseInt(player[1])));
        playerDetail.setOpposition(new Text(player[2]));
        playerDetail.setTimestamps(new
                LongWritable(Long.parseLong(player[3])));
        playerDetail.setBallsTaken(new
                IntWritable(Integer.parseInt(player[4])));
        playerDetail.setFours(new
                IntWritable(Integer.parseInt(player[5])));
        playerDetail.setSix(new IntWritable(Integer.parseInt(player[6])));
        context.write(playerDetail.getPlayerName(), playerDetail);
    }
}