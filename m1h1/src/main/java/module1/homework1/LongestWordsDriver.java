package module1.homework1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import module1.homework1.CustomKey;

/**
 * Accepts input file and output folder paths, defines job properties, runs job
 * @author Oleh
 * @param String [] -> input file and output folder
 *
 */
public class LongestWordsDriver {

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage LongestWordsDriver: <input file> <output folder>");
			System.exit(2);
		}
		Configuration conf = new Configuration();		
		Job job = Job.getInstance(conf, "Longest Words");
		job.setJarByClass(module1.homework1.LongestWordsDriver.class);
		job.setMapperClass(module1.homework1.LongestWordsMapper.class);
		job.setCombinerClass(module1.homework1.LongestWordsReducer.class);
		job.setReducerClass(module1.homework1.LongestWordsReducer.class);
		job.setMapOutputKeyClass(CustomKey.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(CustomKey.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		if (!job.waitForCompletion(true))
			return;
	}

}
