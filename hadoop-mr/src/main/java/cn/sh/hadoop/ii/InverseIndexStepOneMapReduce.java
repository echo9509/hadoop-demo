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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @author sh
 */
public class InverseIndexStepOneMapReduce {

    public static class InverseIndexStepOneMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

        /**
         * Map 输出的键值对为<hello-->a.txt, 1>
         * @param key
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] lineWords = StringUtils.split(value.toString(), " ");
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String fileName = fileSplit.getPath().getName();
            for (String word : lineWords) {
                context.write(new Text(word + "-->" + fileName), new LongWritable(1));
            }
        }
    }

    public static class InverseIndexStepOneReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long counter = 0;
            for (LongWritable value : values) {
                counter += value.get();
            }
            context.write(key, new LongWritable(counter));
        }
    }

    public static class InverseIndexStepOneRunner extends Configured implements Tool {

        @Override
        public int run(String[] args) throws Exception {
            Configuration configuration = new Configuration();
            Job job = Job.getInstance(configuration);

            job.setJarByClass(InverseIndexStepOneMapReduce.class);
            job.setMapperClass(InverseIndexStepOneMapper.class);
            job.setReducerClass(InverseIndexStepOneReducer.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(LongWritable.class);

            FileInputFormat.setInputPaths(job, new Path(args[0]));

            Path outPath = new Path(args[1]);
            FileSystem fileSystem = FileSystem.get(configuration);
            if (fileSystem.exists(outPath)) {
                fileSystem.delete(outPath, true);
            }
            FileOutputFormat.setOutputPath(job, outPath);

            return job.waitForCompletion(true) ? 0 : 1;
        }
    }

    public static void main(String[] args) throws Exception {
        int run = ToolRunner.run(new InverseIndexStepOneRunner(), args);
        System.exit(run);
    }
}
