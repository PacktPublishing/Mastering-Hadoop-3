package com.packt.mapreduce.minmax;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
public class MinMaxReducer extends Reducer<Text, PlayerDetail, Text,
        PlayerReport> {
    PlayerReport playerReport = new PlayerReport();
    @Override
    protected void reduce(Text key, Iterable<PlayerDetail> values, Context
            context) throws IOException, InterruptedException {
        playerReport.setPlayerName(key);
        playerReport.setMaxScore(new IntWritable(0));
        playerReport.setMinScore(new IntWritable(0));
        for (PlayerDetail playerDetail : values) {
            int score = playerDetail.getScore().get();
            if (score > playerReport.getMaxScore().get()) {
                playerReport.setMaxScore(new IntWritable(score));
                playerReport.setMaxScoreopposition(playerDetail.getOpposition());
            }
            if (score < playerReport.getMaxScore().get()) {
                playerReport.setMinScore(new IntWritable(score));
                playerReport.setMinScoreopposition(playerDetail.getOpposition());
            }
            context.write(key, playerReport);
        }
    }
}
