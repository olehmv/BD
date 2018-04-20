package module1.homework1;

import static module1.homework1.StringUtils.stringBuilder;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Group words by length in descending order
 * 
 * @author Oleh
 * @param CustomKey
 *            input key -> word length
 * @param Text
 *            input value -> list of words
 * @param CustomKey
 *            output key -> word length
 * @param Text
 *            output value -> list of words with the same length
 *
 */
public class LongestWordsReducer extends Reducer<CustomKey, Text, CustomKey, Text> {
	private CustomKey wordLength;
	private Text words;
	private final String WORDSEPARATOR = "\t";

	@Override
	protected void reduce(CustomKey key, Iterable<Text> values,
			Reducer<CustomKey, Text, CustomKey, Text>.Context context) throws IOException, InterruptedException {
		wordLength = key;
		words = new Text();
		words.set(stringBuilder(values, WORDSEPARATOR));
		context.write(wordLength, words);
	}
}
