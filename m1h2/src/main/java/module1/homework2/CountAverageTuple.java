package module1.homework2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
/**
 * Writable class to send values across network, helps implements reducer as combiner 
 * @author Oleh
 */
import org.apache.hadoop.io.Writable;

/**
 * Helps to implements reducer as combiner
 * @author Oleh
 *
 */
public class CountAverageTuple implements Writable {
	private float count;
	private float average;

	public CountAverageTuple(float count, float average) {
		super();
		this.count = count;
		this.average = average;
	}

	public CountAverageTuple() {
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		count = in.readFloat();
		average = in.readFloat();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeFloat(count);
		out.writeFloat(average);
	}

	public float getCount() {
		return count;
	}

	public void setCount(float count) {
		this.count = count;
	}

	public float getAverage() {
		return average;
	}

	public void setAverage(float average) {
		this.average = average;
	}

	@Override
	public String toString() {
		return average + "," + (long) count;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Float.floatToIntBits(average);
		result = prime * result + Float.floatToIntBits(count);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		CountAverageTuple other = (CountAverageTuple) obj;
		if (Float.floatToIntBits(average) != Float.floatToIntBits(other.average))
			return false;
		if (Float.floatToIntBits(count) != Float.floatToIntBits(other.count))
			return false;
		return true;
	}

}
