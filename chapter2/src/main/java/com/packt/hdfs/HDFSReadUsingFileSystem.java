package com.packt.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import java.io.InputStream;
import java.net.URI;
public class HDFSReadUsingFileSystem {
    public static void main(String[] args) throws Exception {
        String uri = args[0];
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(URI.create(uri),
                conf);
        InputStream fileInputStream = null;
        try {
            fileInputStream = fileSystem.open(new Path(uri));
            IOUtils.copyBytes(fileInputStream, System.out, 4096,
                    false);
        } finally {
            IOUtils.closeStream(fileInputStream);
        }
    }
}
