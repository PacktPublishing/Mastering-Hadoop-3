package com.packt.mapreduce.mapjoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.*;

import java.net.URI;
import java.util.HashMap;
public class UserPurchaseMapSideJoinMapper extends
        Mapper<LongWritable, Text, Text, Text> {
    private HashMap<String, String> userDetails = new HashMap<String,
            String>();
    private Configuration conf;
    public void setup(Context context) throws IOException {
        conf = context.getConfiguration();
        URI[] URIs = Job.getInstance(conf).getCacheFiles();
        for (URI patternsURI : URIs) {
            Path filePath = new Path(patternsURI.getPath());
            String userDetailFile = filePath.getName();
            readFile(userDetailFile);
        }
    }
    private void readFile(String filePath) {
        try {
            BufferedReader bufferedReader = new BufferedReader(new
                    FileReader(filePath));
            String userInfo = null;
            while ((userInfo = bufferedReader.readLine()) != null) {
 /* Add Record to map here. You can modify value and key
accordingly.*/
                userDetails.put(userInfo.split(",")[0],
                        userInfo.toLowerCase());
            }
        } catch (IOException ex) {
            System.err.println("Exception while reading stop words file: "
                    + ex.getMessage());
        }
    }
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String purchaseDetailUserId = value.toString().split(",")[0];
        String userDetail = userDetails.get(purchaseDetailUserId);
        /*Perform the join operation here*/
    }
}

        