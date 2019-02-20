package cn.sh.hadoop.ii;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @author sh
 */
public class InverseIndexStepTowMapReduce {

    public static class InverseIndexStepTwoMapper extends Mapper<LongWritable, Text, Text, Text> {

        /**
         * 输出数据 <hello, a.txt-->3>
         * @param key
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] lineWords = StringUtils.split(value.toString(), "\t");
            String[] wordAndFileName = StringUtils.split(lineWords[0], "-->");
            context.write(new Text(wordAndFileName[0]), new Text(wordAndFileName[1] + "-->" + lineWords[1]));
        }
    }

    public static class InverseIndexStepTwoReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder outValue = new StringBuilder();
            for (Text value : values) {
                outValue.append(value).append("\t");
            }
            context.write(key, new Text(outValue.toString()));
        }
    }

    public static class InverseIndexStepTwoRunner extends Configured implements Tool {

        @Override
        public int run(String[] args) throws Exception {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf);
            job.setJarByClass(InverseIndexStepTwoRunner.class);
            job.setMapperClass(InverseIndexStepTwoMapper.class);
            job.setReducerClass(InverseIndexStepTwoReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.setInputPaths(job, new Path(args[0]));
            Path outPath = new Path(args[1]);
            FileSystem fs = FileSystem.get(conf);
            if (fs.exists(outPath)) {
                fs.delete(outPath, true);
            }
            FileOutputFormat.setOutputPath(job, outPath);

            return job.waitForCompletion(true) ? 0 : 1;
        }
    }

    public static void main(String[] args) throws Exception {
        int run = ToolRunner.run(new InverseIndexStepTwoRunner(), args);
        System.exit(run);
    }
}