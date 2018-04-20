package module1.homework2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.BasicConfigurator;

/**
 * Accepts input file and output folder paths, defines job properties, runs job
 * 
 * @author
 * @param String
 *            [] -> input file and output folder
 *
 */
public class LogDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("mapreduce.output.textoutputformat.separator", ",");
		Job job = Job.getInstance(conf, "Byte avarege/count job");
		job.setJarByClass(module1.homework2.LogDriver.class);
		job.setMapperClass(module1.homework2.LogMapper.class);
		job.setCombinerClass(module1.homework2.LogReducer.class);
		job.setReducerClass(module1.homework2.LogReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(CountAverageTuple.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(CountAverageTuple.class);
		TextInputFormat.setInputPaths(job, new Path(args[0]));
		if (args.length == 3 && args[2].trim().equals("compress")) {
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
			SequenceFileOutputFormat.setCompressOutput(job, true);
			SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
			conf.set("mapreduce.output.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec");
		} else {
			TextOutputFormat.setOutputPath(job, new Path(args[1]));

		}
		TextOutputFormat.setOutputPath(job, new Path(args[1]));
		if (!job.waitForCompletion(true))
			return;
	}

}
