package cn.sh.hadoop.flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author sh
 */
public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

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
