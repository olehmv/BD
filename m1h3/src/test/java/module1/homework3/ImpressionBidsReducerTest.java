package module1.homework3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.util.stream.Collectors;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class ImpressionBidsReducerTest {

	ImpressionBidsReducer reducer;
	ReduceDriver<CustomKey, LongWritable, CustomKey, LongWritable> driver;

	@Before
	public void setUp() throws Exception {
		reducer = new ImpressionBidsReducer();
		driver = ReduceDriver.newReduceDriver(reducer);
	}
	
	
	
	@Test
	public void impressionBidsReducerTest() throws IOException {
		driver.withAll(readInput());
		driver.withAllOutput(readOutput());
		driver.runTest();
	}
	
	private List<Pair<CustomKey, LongWritable>> readOutput() {
		List<Pair<CustomKey, LongWritable>> output = new ArrayList<>();
		@SuppressWarnings("resource")
		Scanner scanner = new Scanner(ImpressionBidsMapper.class.getResourceAsStream("/output/r_out.txt"))
				.useDelimiter("\n");
		while (scanner.hasNext()) {
			String[] out = scanner.next().split("\t");
			output.add(new Pair<CustomKey, LongWritable>(new CustomKey(out[0].trim(),out[1].trim()),
					new LongWritable(Long.valueOf(out[2].trim()))));
		}
		scanner.close();
		return output;
	}
	
	private List<Pair<CustomKey, List<LongWritable>>> readInput() {
		List<Pair<CustomKey, List<LongWritable>>> input = new ArrayList<>();
		CustomKey key = new CustomKey();
		List<LongWritable> values = new ArrayList<>();
		@SuppressWarnings("resource")
		Scanner scanner = new Scanner(ImpressionBidsReducerTest.class.getResourceAsStream("/output/m_out.txt"))
				.useDelimiter("\n");
		while (scanner.hasNext()) {
			String[] out = scanner.next().split("\t");
			key.setOs(out[0].split(",")[0].trim());
			key.setCity(out[1].trim());
			values.add(new LongWritable(Long.valueOf(out[2].trim())));
			
		input.add(new Pair<CustomKey, List<LongWritable>>(key, values));
		}
		scanner.close();
		return input;
	}
}

