package cn.sh.hadoop.flow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author sh
 */
public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

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
