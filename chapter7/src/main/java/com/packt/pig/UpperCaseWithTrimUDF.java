package com.packt.pig;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import java.io.IOException;
public class UpperCaseWithTrimUDF extends EvalFunc<String> {
    @Override
    public String exec(Tuple tuple) throws IOException {
        if (tuple == null || tuple.size() == 0)
            return null;
        try {
            String inputRecord = (String) tuple.get(0);


            inputRecord = inputRecord.trim();
            inputRecord = inputRecord.toUpperCase();
            return inputRecord;
        } catch (Exception ex) {
            throw new IOException("unable to trim and convert to upper case ", ex);
        }
    }
}