package com.packt.pig;

import org.apache.pig.FilterFunc;
import org.apache.pig.data.Tuple;
import java.io.IOException;
public class IsLengthGreaterThen50 extends FilterFunc {
    @Override
    public Boolean exec(Tuple tuple) throws IOException {
        if (tuple == null || tuple.size() == 0)
            return false;
        String inputRecord = (String) tuple.get(0);
        return inputRecord.length()>=50;
    }
}