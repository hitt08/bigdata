package mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class Mapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, IntWritable>{
	static enum Counters {NUM_RECORDS, NUM_LINES, NUM_BYTES}
	private Text _key = new Text();
	private IntWritable _value = new IntWritable();
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		StringTokenizer tokenizer = new StringTokenizer(value.toString(), "\n");
		while(tokenizer.hasMoreTokens()) {
			String line = tokenizer.nextToken();
			int sep = line.indexOf(" ");
			_key.set((sep == -1) ? line : line.substring(0, line.indexOf(" ")));
			_value.set(1);
			context.write(_key, _value);
			context.getCounter(Counters.NUM_LINES).increment(1);
		}
		context.getCounter(Counters.NUM_BYTES).increment(value.getLength());
		context.getCounter(Counters.NUM_RECORDS).increment(1);
	}
}
