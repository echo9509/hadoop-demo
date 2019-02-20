package cn.sh.hadoop.group;

import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;
import java.util.Map;

/**
 * @author sh
 */
public class GroupPartitioner<KEY, VALUE> extends Partitioner<KEY, VALUE> {

    private static Map<String, Integer> group;

    static {
        group = new HashMap<>();
        loadResource(group);
    }

    private static void loadResource(Map<String, Integer> group) {
        group.put("136", 0);
        group.put("137", 1);
        group.put("138", 2);
        group.put("139", 3);
        group.put("135", 4);
    }

    @Override
    public int getPartition(KEY key, VALUE value, int numPartitions) {
        String prefix = key.toString().substring(0, 3);
        return group.get(prefix) == null ? 5 : group.get(prefix);
    }
}
