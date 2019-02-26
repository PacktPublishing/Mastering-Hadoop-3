package com.packt.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
public class HDFSFileSystemWrite {
    public static void main(String[] args) throws IOException {
        String sourceURI = args[0];
        String targetURI = args[1];
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(targetURI), conf);
        FSDataOutputStream out = null;
        InputStream in = new BufferedInputStream(new
                FileInputStream(sourceURI));
        try {
            out = fs.create(new Path(targetURI));
            IOUtils.copyBytes(in, out, 4096, false);
        } finally {
            in.close();
            out.close();
        }
    }
}
