package com.packt.hdfs;

import java.io.InputStream;
import java.net.URL;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;
public class HDFSReadUsingURL {
    static {
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
    }
    public static void main(String[] args) throws Exception {
        InputStream fileInputStream = null;
        try {
            fileInputStream = new URL(args[0]).openStream();
            IOUtils.copyBytes(fileInputStream, System.out, 4096,
                    false);
        } finally {
            IOUtils.closeStream(fileInputStream);
        }
    }
}