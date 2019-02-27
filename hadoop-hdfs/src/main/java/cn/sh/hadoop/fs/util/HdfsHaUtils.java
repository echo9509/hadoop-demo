package cn.sh.hadoop.fs.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author sh
 */
public class HdfsHaUtils {

    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        conf.addResource("core-site.xml");
        conf.addResource("hdfs-sitec.xml");
        FileSystem fs = FileSystem.get(new URI("hdfs://ns1"), conf, "hadoop");
        fs.copyFromLocalFile(new Path("/Users/sh/Desktop/Hadoop权威指南大数据的存储与分析 第4版.pdf"), new Path("/"));
    }
}
