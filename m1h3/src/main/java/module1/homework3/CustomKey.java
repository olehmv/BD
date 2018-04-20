package module1.homework3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
/**
 * Custom key operation system & city name holder
 * @author Oleh
 *
 */
public class CustomKey implements WritableComparable<CustomKey> {
	private String os;
	private String city;

	public CustomKey(String os, String city) {
		setOs(os);
		setCity(city);
	}

	public CustomKey() {
		super();
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		setOs(in.readUTF());
		setCity(in.readUTF());
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(os);
		out.writeUTF(city);
	}

	@Override
	public int compareTo(CustomKey o) {
		int result = os.toString().compareTo(o.getOs().toString());
		if (0 == result) {
			result = city.toString().compareTo(o.getCity().toString());
		}
		return result;
	}

	public String getOs() {
		return os;
	}

	public void setOs(String os) {
		this.os = os;
	}

	public String getCity() {
		return city;
	}

	public void setCity(String city) {
		this.city = city;
	}

	@Override
	public String toString() {
		return os + "\t" + city;
	}

	@Override
	public int hashCode() {
		return os.hashCode() * 31 +city.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof CustomKey) {
			CustomKey key = (CustomKey) obj;
			return getCity().equals(key.getCity())
					&& getOs().equals(key.getOs());
		}
		return false;

	}

}
