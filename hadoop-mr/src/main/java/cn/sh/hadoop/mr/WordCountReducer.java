package cn.sh.hadoop.mr;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer程序用来汇总结果
 * MR框架会将map产生的结果进行缓存，等待所有的map程序完成之后，才进行Reduce处理
 *
 * @author sh
 */
public class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    /**
     * MR框架会对map产生的结果进行分组排序
     * 以组的形式发送给Reducer程序，每发一组，调用一次Reducer的reduce方法
     *
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        final Long[] wordCount = {0L};
        values.forEach(count -> wordCount[0] += count.get());
        context.write(key, new LongWritable(wordCount[0]));
    }
}
