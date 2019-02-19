package cn.sh.hadoop.sort;

import cn.sh.hadoop.flow.FlowBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class SortMapReduce {

    public static class SortMapper extends Mapper<LongWritable, Text, FlowBean, NullWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] lineValues = value.toString().split("\t");
            long upFlow = Long.parseLong(lineValues[1]);
            long downFlow = Long.parseLong(lineValues[2]);
            FlowBean flow = new FlowBean(lineValues[0], upFlow, downFlow);
            context.write(flow, NullWritable.get());
        }
    }

    public static class SortReducer extends Reducer<FlowBean, NullWritable, Text, FlowBean> {
        @Override
        protected void reduce(FlowBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(new Text(key.getPhoneNumber()), key);
        }
    }

    public static class SortRunner extends Configured implements Tool {

        @Override
        public int run(String[] args) throws Exception {
            Configuration configuration = new Configuration();
            Job job = Job.getInstance(configuration);

            job.setJarByClass(SortMapper.class);
            job.setMapperClass(SortMapper.class);
            job.setReducerClass(SortReducer.class);

            job.setMapOutputKeyClass(FlowBean.class);
            job.setMapOutputValueClass(NullWritable.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(FlowBean.class);

            FileInputFormat.setInputPaths(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            return job.waitForCompletion(true) ? 0 : 1;
        }
    }

    public static void main(String[] args) throws Exception {
        int run = ToolRunner.run(new SortRunner(), args);
        System.exit(run);
    }
 }
