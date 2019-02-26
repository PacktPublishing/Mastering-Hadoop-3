package com.packt.mapreduce.topk;

import com.packt.mapreduce.minmax.PlayerDetail;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class TopKMapper extends
        Mapper<LongWritable, Text, IntWritable, PlayerDetail> {
    private int K = 10;
    private TreeMap<Integer, PlayerDetail> topKPlayerWithLessScore = new
            TreeMap<Integer, PlayerDetail>();
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
        topKPlayerWithLessScore.put(playerDetail.getScore().get(),
                playerDetail);

        if (topKPlayerWithLessScore.size() > K) {
            topKPlayerWithLessScore.remove(topKPlayerWithLessScore.lastKey());
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException,
            InterruptedException {
        for (Map.Entry<Integer, PlayerDetail> playerDetailEntry :
                topKPlayerWithLessScore.entrySet()) {
            context.write(new IntWritable(playerDetailEntry.getKey()),
                    playerDetail);
        }
    }
}