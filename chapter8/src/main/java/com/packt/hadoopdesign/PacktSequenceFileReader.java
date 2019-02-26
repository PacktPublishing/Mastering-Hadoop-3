package com.packt.hadoopdesign;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;

public class PacktSequenceFileReader {
    public static void main(String[] args) throws IOException {


    String uri = args[0];
    Configuration conf = new Configuration();
    Path path = new Path(uri);
    SequenceFile.Reader sequenceReader = null;
	try {
        sequenceReader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(path), SequenceFile.Reader.bufferSize(4096), SequenceFile.Reader.start(0));
        Writable key = (Writable) ReflectionUtils.newInstance(sequenceReader.getKeyClass(), conf);
        Writable value = (Writable) ReflectionUtils.newInstance(sequenceReader.getValueClass(), conf);
        while (sequenceReader.next(key, value)) {
            String syncSeen = sequenceReader.syncSeen() ? "*" : "";
            System.out.printf("[%s]\t%s\t%s\n", syncSeen, key, value);
        }
    } finally {
        IOUtils.closeStream(sequenceReader);
    }
}
}

