package com.packt.mapreduce.topk;

import com.packt.mapreduce.minmax.PlayerDetail;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
public class TopKReducer extends Reducer<IntWritable, PlayerDetail,
        IntWritable, PlayerDetail> {
    private int K = 10;
    private TreeMap<Integer, PlayerDetail> topKPlayerWithLessScore = new
            TreeMap<Integer, PlayerDetail>();
    private PlayerDetail playerDetail = new PlayerDetail();
    @Override
    protected void reduce(IntWritable key, Iterable<PlayerDetail> values,
                          Context context) throws IOException, InterruptedException {
        for (PlayerDetail playerDetail : values) {
            topKPlayerWithLessScore.put(key.get(), playerDetail);
            if (topKPlayerWithLessScore.size() > K) {
                topKPlayerWithLessScore.remove(topKPlayerWithLessScore.lastKey());
            }
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
