package module1.homework3;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class ImpressionBidsDriverTest {

	ImpressionBidsMapper mapper;
	ImpressionBidsReducer reducer;
	MapReduceDriver<LongWritable, Text,CustomKey,LongWritable, CustomKey,LongWritable> mapReduceDriver;
	CustomKeyWritableComparator keyComparator;
	CustomKeyGroupingComparator customKeyGroupingComparator;
	@Before
	public void setUp() throws Exception {
		mapper = new ImpressionBidsMapper();
		reducer = new ImpressionBidsReducer();
		keyComparator=new CustomKeyWritableComparator();
		customKeyGroupingComparator=new CustomKeyGroupingComparator();
		mapReduceDriver=new MapReduceDriver<>();
	}
	public  File getCacheFile() {
		ClassLoader classLoader = getClass().getClassLoader();
		File file = new File(classLoader.getResource("cache/city.en.txt").getFile());
		return file;
	}
	@SuppressWarnings("unchecked")
	@Test
	public void impressionBidsDriverTest() throws IOException {
			mapReduceDriver.withAll(readInput());
			mapReduceDriver.withAllOutput(readOutput());
			mapReduceDriver.withCacheFile(getCacheFile().toURI());
			mapReduceDriver.withKeyOrderComparator(keyComparator);
			mapReduceDriver.withKeyGroupingComparator(customKeyGroupingComparator);
			mapReduceDriver.withMapper(mapper);
			mapReduceDriver.withReducer(reducer);
			mapReduceDriver.runTest();
	}
	
	public  List<Pair<LongWritable, Text>> readInput() {
		List<Pair<LongWritable, Text>> input = new ArrayList<>();
		@SuppressWarnings("resource")
		Scanner scanner = new Scanner(ImpressionBidsMapperTest.class.getResourceAsStream("/input/m_input.txt"))
				.useDelimiter("\n");
		while (scanner.hasNext()) {
			input.add(new Pair<LongWritable, Text>(new LongWritable(), new Text(scanner.next().trim())));
		}
		scanner.close();
		return input;
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

}
