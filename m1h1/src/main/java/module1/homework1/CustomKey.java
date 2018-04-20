package module1.homework1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * CustomKey -> defines a descending order in the shuffle/sort phase
 * 
 * @author Oleh
 */
public class CustomKey implements WritableComparable<CustomKey> {
	private int key;

	public CustomKey(int k) {
		key = k;
	}

	public CustomKey() {

	}

	@Override
	public void readFields(DataInput in) throws IOException {
		set(in.readInt());
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(key);
	}

	@Override
	public int compareTo(CustomKey k) {
		if (k.get() > key) {
			return 1;
		}
		if (k.get() < key) {
			return -1;
		}
		return 0;

	}

	public int get() {
		return key;
	}

	public void set(int k) {
		key = k;
	}

	public String toString() {

		return Integer.toString(key);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + key;
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
		CustomKey other = (CustomKey) obj;
		if (key != other.key)
			return false;
		return true;
	}

}
