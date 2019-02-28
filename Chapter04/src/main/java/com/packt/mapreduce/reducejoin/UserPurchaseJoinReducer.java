package com.packt.mapreduce.reducejoin;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.ArrayList;
public class UserPurchaseJoinReducer extends Reducer<Text, Text, Text,
        Text> {
    private Text tmp = new Text();
    private ArrayList<Text> userList = new ArrayList<Text>();
    private ArrayList<Text> purchaseList = new ArrayList<Text>();
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        userList.clear();
        purchaseList.clear();
        while (values.iterator().hasNext()) {
            tmp = values.iterator().next();
            if (tmp.charAt(0) == 'X') {
                userList.add(new Text(tmp.toString().substring(1)));
            } else if (tmp.charAt('0') == 'Y') {
                purchaseList.add(new Text(tmp.toString().substring(1)));
            }
        }

        /* Joining both dataset */

        if (!userList.isEmpty() && !purchaseList.isEmpty()) {
            for (Text user : userList) {
                for (Text purchase : purchaseList) {
                    context.write(user, purchase);
                }
            }
        }
    }
}
