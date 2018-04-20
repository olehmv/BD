package module1.homework3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Set custom partitioner, custom key comparator, custom key, custom grouping
 * key comparator, accept file from cache, run the job
 * 
 * @author Oleh
 *
 */
public class ImpressionBidsDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, " Impression Bids Driver job");
		job.setJarByClass(module1.homework3.ImpressionBidsDriver.class);
		job.setMapperClass(module1.homework3.ImpressionBidsMapper.class);
		job.setPartitionerClass(module1.homework3.CustomPartitioner.class);
		job.setSortComparatorClass(module1.homework3.CustomKeyWritableComparator.class);
		job.setGroupingComparatorClass(module1.homework3.CustomKeyGroupingComparator.class);
		job.setMapOutputKeyClass(module1.homework3.CustomKey.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setReducerClass(module1.homework3.ImpressionBidsReducer.class);
		job.setOutputKeyClass(module1.homework3.CustomKey.class);
		job.setOutputValueClass(LongWritable.class);
		TextInputFormat.setInputPaths(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));
		job.addCacheFile(new Path(args[2]).toUri());
		job.setNumReduceTasks(4);

		if (!job.waitForCompletion(true))
			return;
	}

}
