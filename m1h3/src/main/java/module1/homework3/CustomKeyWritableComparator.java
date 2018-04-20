package module1.homework3;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
/**
 * Compares key for reducers
 * @author Oleh
 *
 */
public class CustomKeyWritableComparator extends WritableComparator {

	protected CustomKeyWritableComparator() {
		super(CustomKey.class, true);
	}
	/**
	 * Sort keys as (Operation System + City name) alphabetically
	 */
	@Override
	@SuppressWarnings("rawtypes")
	public int compare(WritableComparable w1, WritableComparable w2) {
		CustomKey key1 = (CustomKey) w1;
		CustomKey key2 = (CustomKey) w2;
		return key1.getOs().concat(key1.getCity()).compareTo(key2.getOs().concat(key2.getCity()));
	}

}
