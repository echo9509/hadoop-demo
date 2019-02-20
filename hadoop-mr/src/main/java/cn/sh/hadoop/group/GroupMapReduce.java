package cn.sh.hadoop.group;

import cn.sh.hadoop.flow.FlowBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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

public class GroupMapReduce {

    public static class GroupMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] lineValues = line.split("\t");
            int length = lineValues.length;
            String phoneNumber = lineValues[1];
            long upFlow = Long.parseLong(lineValues[length - 3]);
            long downFlow = Long.parseLong(lineValues[length - 2]);
            context.write(new Text(phoneNumber), new FlowBean(phoneNumber, upFlow, downFlow));
        }
    }

    public static class GroupReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
        @Override
        protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
            long upFlowCounter = 0;
            long downFlowCounter = 0;
            for (FlowBean flow : values) {
                upFlowCounter += flow.getUpFlow();
                downFlowCounter += flow.getDownFlow();
            }
            context.write(key, new FlowBean(key.toString(), upFlowCounter, downFlowCounter));
        }
    }

    public static class GroupRunner extends Configured implements Tool {

        @Override
        public int run(String[] args) throws Exception {
            Configuration configuration = new Configuration();
            Job job = Job.getInstance(configuration);

            job.setPartitionerClass(GroupPartitioner.class);
            job.setNumReduceTasks(6);

            job.setJarByClass(GroupMapReduce.class);

            job.setMapperClass(GroupMapper.class);
            job.setReducerClass(GroupReducer.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(FlowBean.class);

            FileInputFormat.setInputPaths(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            return job.waitForCompletion(true) ? 0 : 1;
        }
    }

    public static void main(String[] args) throws Exception {
        int run = ToolRunner.run(new GroupRunner(), args);
        System.exit(run);
    }
}
