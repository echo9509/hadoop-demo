package cn.sh.hadoop.mr;


import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Map程序需要继承Mapper类，该类需要指定四个泛型参数
 * 分别为Map程序的输入key-value和输出key-value
 * 一般Map程序的输入参数key是读取文件的起始偏移量，输入值是文件的每一行数据
 * Map程序的输出参数的kv类型需要和Reduce程序的输入参数kv的类型一致
 * @author sh
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    /**
     * Map框架每读一行数据都会调用一次Mapper的map方法
     *
     * @param key 文件的偏移量
     * @param value 每一行数据
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String lineValue = value.toString();
        String[] words = StringUtils.split(lineValue, " ");
        for (String word : words) {
            context.write(new Text(word), new LongWritable(1));
        }
    }
}
