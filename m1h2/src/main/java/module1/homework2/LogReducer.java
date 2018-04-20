package module1.homework2;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Write out average an count of bytes sent per ip address
 * 
 * @author Oleh
 * @param Text
 *            input key -> ip address
 * @param CountAverageTuple
 *            input value -> tuple of count 1 and bytes sent
 * @param Text
 *            output key -> ip address
 * @param CountAverageTuple
 *            output value -> sum of bytes sent and average of the bytes sent
 *            per ip address
 *
 */
public class LogReducer extends Reducer<Text, CountAverageTuple, Text, CountAverageTuple> {
	private CountAverageTuple result = new CountAverageTuple();;

	@Override
	protected void reduce(Text key, Iterable<CountAverageTuple> values,
			Reducer<Text, CountAverageTuple, Text, CountAverageTuple>.Context context)
			throws IOException, InterruptedException {
		float sum = 0;
		float count = 0;
		for (CountAverageTuple val : values) {
			sum += val.getCount() * val.getAverage();
			count += val.getCount();

		}
		result.setCount(sum);
		result.setAverage(sum / count);
		context.write(key, result);
	}

}
