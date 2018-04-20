package module1.homework3;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reads output from Mappers as CustomKey(operation system+city name,city name) 
 * to properly count number of bids per city 
 * Write out CustomKey(operation system, city name) & number of bids per city
 * @author Oleh
 *
 */
public class ImpressionBidsReducer extends Reducer<CustomKey, LongWritable, CustomKey, LongWritable> {

	private final LongWritable outValue = new LongWritable();

	@Override
	protected void reduce(CustomKey key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {

		
		long amount = 0;
		for (@SuppressWarnings("unused")
		LongWritable bid : values) {
		amount++;
		}
		outValue.set(amount);
		key.setOs(key.getOs().split(",")[0]);
		context.write(key, outValue);

	}

}
