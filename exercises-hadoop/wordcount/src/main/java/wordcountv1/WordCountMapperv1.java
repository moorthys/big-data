package wordcountv1;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jackson.map.ObjectMapper;

import wordcountv1.model.ReviewRecord;

public class WordCountMapperv1 extends
		Mapper<LongWritable, Text, Text, IntWritable> {
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		ObjectMapper mapper = new ObjectMapper();
		ReviewRecord reviewRecord = mapper.readValue(value.toString(),
				ReviewRecord.class);
		StringTokenizer itr = new StringTokenizer(reviewRecord.reviewText);
		while (itr.hasMoreTokens()) {
			word.set(itr.nextToken());
			context.write(word, one);
		}
	}
}
