package module1.homework3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;
/**
 * Partitions by Operation System
 * @author Oleh
 *
 */
public class CustomPartitioner extends Partitioner<CustomKey, LongWritable> {

	@Override
	public int getPartition(CustomKey key, LongWritable value, int numPartitions) {
		if(key.getOs().split(",")[0].trim().equals("Linux")) {
			return Math.abs(numPartitions-1);
		}
		if(key.getOs().split(",")[0].trim().equals("Android")) {
			return Math.abs(numPartitions-2);
		}
		if(key.getOs().split(",")[0].trim().equals("Mac OS X")) {
			return Math.abs(numPartitions-3);
		}
		if(key.getOs().split(",")[0].trim().equals("Windows")) {
			return Math.abs(numPartitions-4);
		}
		return Math.abs(key.getOs().split(",")[0].hashCode()) % numPartitions;
	}

}
