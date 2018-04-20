package module1.homework1;

import static module1.homework1.StringUtils.breakLineToWords;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 * Map word length to word
 * @author Oleh
 *@param LongWritable input key -> line byte offset
 *@param Text input value -> content of the line
 *@param CustomKey output key -> length of the word, key defines a descending order
 *@param Text output value -> word from line
 *  
 */

public class LongestWordsMapper extends Mapper<LongWritable, Text, CustomKey, Text> {
	private CustomKey wordLength;
	private Text word;

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, CustomKey, Text>.Context context)
			throws IOException, InterruptedException {
		String textLine = value.toString();
		String[] wordsArr = breakLineToWords(textLine);
		for (String w : wordsArr) {
			wordLength = new CustomKey();
			wordLength.set(w.length());
			word = new Text();
			word.set(w);
			context.write(wordLength, word);
		}

	}
}
