package cn.sh.hadoop.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Job作业描述类
 * @author sh
 */
public class WordCountRunner {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(WordCountRunner.class);

        //指定Map程序
        job.setMapperClass(WordCountMapper.class);

        //指定Reduce程序
        job.setReducerClass(WordCountReducer.class);


        //设置Reduce程序输出kv的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //设置Map程序输出kv的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //设置MR程序的输入文件的路径
        FileInputFormat.setInputPaths(job, new Path("hdfs://hadoop001:9000/word_count/input"));
        //设置MR程序的输出文件的路径
        FileOutputFormat.setOutputPath(job, new Path("hdfs://hadoop001:9000/word_count/output"));

        //提交MR程序并且打印处理过程
        job.waitForCompletion(true);
    }
}
