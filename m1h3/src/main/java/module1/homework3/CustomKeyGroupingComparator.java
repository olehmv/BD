package module1.homework3;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
/**
 *Groups key for reducers
 * @author Oleh
 *
 */
public class CustomKeyGroupingComparator extends WritableComparator {
	
	protected CustomKeyGroupingComparator() {
		super(CustomKey.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		CustomKey key1 = (CustomKey) w1;
		CustomKey key2 = (CustomKey) w2;
		return key1.getOs().compareTo(key2.getOs());
}

}
