package mapreduce.Practice;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.*;
import java.io.*;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCount3 extends Configured implements Tool{
	
	
	public static class MyPartitioner extends Partitioner<Text, IntWritable>{
		public int getPartition(Text key, IntWritable value, int numPartitions) {
			int c = Character.toLowerCase(key.toString().charAt(0));
			if (c < 'a' || c > 'z')
				return numPartitions - 1;
			return (int) Math.floor ((float) (numPartitions -2 ) * (c - 'a') / ('z' - 'a'));
		}
	}
	
	public static class MyInputFormat extends FileInputFormat<LongWritable, Text>{
		public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context){
			return new MyRecordReader();
		}
	}
	
	
	///////////////////? MY RECORD READER//////////////////////
	public static class MyRecordReader extends RecordReader<LongWritable, Text>{
		private static final byte[] recordSeparator = "\t\t\t".getBytes();
		private FSDataInputStream fsin;
		private long start, end;
		private boolean stillInChunk = true;
		private DataOutputBuffer buffer = new DataOutputBuffer();
		private LongWritable key = new LongWritable();
		private Text value = new Text();
		
		public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException {
			FileSplit split = (FileSplit) inputSplit;
			Configuration conf = context.getConfiguration();
			Path path = split.getPath();
			org.apache.hadoop.fs.FileSystem fs = path.getFileSystem(conf);
			
			fsin = fs.open(path);
			start = split.getStart();
			end = split.getStart() + split.getLength();
			fsin.seek(start);
			
			if(start != 0)
				readRecord(false);
		}
		private boolean readRecord(boolean withinBlock) throws IOException{
			int i = 0, b;
			while(true) {
				if ((b = fsin.read()) == -1) return false;
				if (withinBlock) buffer.write(b);
				if (b == recordSeparator[i]) {
					if(++i == recordSeparator.length)
						return fsin.getPos() < end;
				} else i = 0;
			}
		}
		
		public boolean nextKeyValue() throws IOException{
			if (!stillInChunk) return false;
			boolean status = readRecord(true);
			value = new Text();
			value.set(buffer.getData(), 0, buffer.getLength());
			key.set(fsin.getPos());
			buffer.reset();
			if (!status)
				stillInChunk = false;
			return true;
		}
		public LongWritable getCurrentKey() {return key;}
		public Text getCurrentValue() {return value;}
		public float getProgress() throws IOException{
			return (float) (fsin.getPos() - start) / (end - start);
		}
		public void close() throws IOException {fsin.close();}
		
		
		
	}
	///////////////////// MAPPER//////////////////////////////////
	
	public static class Map extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, IntWritable>{
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
	
	
	
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>{
		private IntWritable _value = new IntWritable();
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
			int sum = 0;
			for (Iterator<IntWritable> it = values.iterator(); it.hasNext();) {
				sum += it.next().get();
			}
			_value.set(sum);
			context.write(key, _value);
		}
	}
	
	
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Job job = Job.getInstance(getConf(), "WordCount-3");
		job.setJarByClass(WordCount3.class);
		
		job.setMapperClass(Map.class);
		job.setPartitionerClass(MyPartitioner.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		
		job.setInputFormatClass(MyInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		return job.waitForCompletion(true) ? 0 : 1;
		
	}
	public static void main(String[] args) throws Exception{
		System.exit(ToolRunner.run(new Configuration(), new WordCount3(), args));
	}
	

}
