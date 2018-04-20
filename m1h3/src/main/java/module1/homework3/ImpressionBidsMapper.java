package module1.homework3;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import eu.bitwalker.useragentutils.UserAgent;

/**
 * Read file from cache & save in hash map
 * Filter out lines with bid's price less then 250
 * Look up city id in hash map if finds writes out:
 * CustomKey(operation system,city name) -> where operation system compose of (operation system + city name) to help reducer 
 * Write out ComposityKey(os,city)& high-bid-price   
 * @author Oleh
 *
 */
public class ImpressionBidsMapper extends Mapper<LongWritable, Text, CustomKey, LongWritable> {

	private final int threshold = 250;
	private final CustomKey outKey = new CustomKey();
	private final LongWritable outValue = new LongWritable();
	protected static final Map<String, String> cities = new HashMap<>();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		URI[] cacheFilesLocal = context.getCacheFiles();
        for (URI eachPath : cacheFilesLocal) {
            Path path = new Path(eachPath);
            BufferedReader reader = null;
    		String line = "";
    		FileSystem fs = FileSystem.get(context.getConfiguration());
    		reader = new BufferedReader(new InputStreamReader(fs.open(path)));
    		while ((line = reader.readLine()) != null) {
    			String tokens[];
    			if(line.contains("unknown")) {
    				tokens = line.split(" ");
    			}else {				
    				tokens = line.split("\t");
    			}
    			cities.put(tokens[0].trim(), tokens[1].trim());
    		}
        }

	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] tokens = value.toString().split("\t");
		int cityID;
		int bidPrice;
		try {
			cityID = Integer.valueOf(tokens[7]);
			bidPrice = Integer.valueOf(tokens[17]);
		} catch (NumberFormatException ex) {
			return;
		}
		String agent = tokens[4];
        UserAgent userAgent = new UserAgent(agent);
		if (bidPrice > threshold) {
			String id = String.valueOf(cityID);
			if(cities.containsKey(id)) {
				String operatingSystem = userAgent.getOperatingSystem().getGroup().getName();
				outKey.setOs(operatingSystem+","+cities.get(id));
				outKey.setCity(cities.get(id));
				outValue.set(bidPrice);
				context.write(outKey, outValue);
			}
		}
	}
}