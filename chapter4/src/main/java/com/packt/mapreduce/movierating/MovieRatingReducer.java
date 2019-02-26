package com.packt.mapreduce.movierating;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
public class MovieRatingReducer extends Reducer<Text, Text, Text, Text> {
    private int K = 20;
    private TreeMap<String, String> topMiviesByRating = new TreeMap<>();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        for (Text movie : values) {
            topMiviesByRating.put(key.toString(), movie.toString());
            if (topMiviesByRating.size() > K) {
                topMiviesByRating.remove(topMiviesByRating.firstKey());
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException,
            InterruptedException {
        for (Map.Entry<String, String> movieDetail :
                topMiviesByRating.entrySet()) {
            context.write(new Text(movieDetail.getKey()), new
                    Text(movieDetail.getValue()));
        }
    }


}
