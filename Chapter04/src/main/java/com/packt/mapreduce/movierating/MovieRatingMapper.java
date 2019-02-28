package com.packt.mapreduce.movierating;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
public class MovieRatingMapper extends
        Mapper<LongWritable, Text, Text, Text> {
    private int K = 10;
    private TreeMap<String, String> movieMap = new TreeMap<>();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] line_values = value.toString().split("\t");
        String movie_title = line_values[0];
        String movie_rating = line_values[1];
        int noOfPeople = Integer.parseInt(line_values[2]);
        if (noOfPeople > 100) {
            movieMap.put(movie_title, movie_rating);
            if (movieMap.size() > K) {
                movieMap.remove(movieMap.firstKey());
            }
        }
    }


    @Override
    protected void cleanup(Context context) throws IOException,
            InterruptedException {


        for (Map.Entry<String, String> movieDetail : movieMap.entrySet()) {
            context.write(new Text(movieDetail.getKey()), new
                    Text(movieDetail.getValue()));
        }
    }
}