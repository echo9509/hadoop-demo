package cn.sh.hadoop.fs.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author sh
 */
public class HdfsUtils {

    private FileSystem fs;

    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        Configuration configuration = new Configuration();
        fs = FileSystem.get(new URI("hdfs://hadoop001:9000"), configuration, "hadoop");
    }

    /**
     * 上传文件
     */
    @Test
    public void uploadFile() throws IOException {
        Path dstPath = new Path("/");
        Path srcPath = new Path("/Users/sh/Desktop/jdk-8u201-linux-x64.tar.gz");
        fs.copyFromLocalFile(srcPath, dstPath);
    }


    /**
     * 下载文件
     */
    @Test
    public void downLoadFile() throws IOException {
        Path srcPath = new Path("/jdk-8u201-linux-x64.tar.gz");
        Path dstPath = new Path("/Users/sh/workspace/hadoop-demo/");
        fs.copyToLocalFile(srcPath, dstPath);
    }

    /**
     * 递归创建文件夹
     */
    @Test
    public void mkdir() throws IOException {
        Path src = new Path("/aa/bb/cc");
        fs.mkdirs(src);
    }


    /**
     * 删除文件
     * @throws IOException
     */
    @Test
    public void removeFile() throws IOException {
        Path src = new Path("/aa/bb");
        fs.delete(src, true);
    }

    /**
     * 展示文件信息
     */
    @Test
    public void listFiles() throws IOException {
        Path path = new Path("/");
        RemoteIterator<LocatedFileStatus> fileList = fs.listFiles(path, true);
        LocatedFileStatus file;
        while (fileList.hasNext()) {
            file = fileList.next();
            System.out.println(file.getPath().getName());
        }
    }

    /**
     * 列出给定路径下的文件和文件夹信息
     */
    @Test
    public void listDir() throws IOException {
        Path path = new Path("/");
        FileStatus[] fileList = fs.listStatus(path);
        for (FileStatus file : fileList) {
            System.out.println(file.getPath().getName() + " is " + (file.isDirectory() ? " directory" : " file"));
        }
    }
}
