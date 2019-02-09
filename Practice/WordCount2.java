package mapreduce.Practice;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.*;
import java.io.*;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCount2 extends Configured implements Tool{
	
	public static class Map extends Mapper<LongWritable, Text, Text, LongWritable>{
		static enum Counters {INPUT_WORDS}
		
		private final static LongWritable one = new LongWritable(1);
		private Text word = new Text();
		private boolean caseSensitive = true;
		private Set<String> patternsToSkip = new HashSet<String>();
		private long numRecords = 0;
		static long counter = 0;
		private String inputFile;
		
		private void parseSkipFile (Path patternsFile) {
			try {
				BufferedReader fis = new BufferedReader(new FileReader (patternsFile.toString()));
				String pattern = null;
				while((pattern = fis.readLine()) != null)
					patternsToSkip.add(pattern);
				fis.close();
			} catch(IOException ioe) {
				System.err.println("Caught exception while parsing the cahced file'"+ patternsFile);
			}
		}
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			caseSensitive = conf.getBoolean("wordcount.case.sensitive", true);
			inputFile = conf.get("map.input.file");
			if (conf.getBoolean("wordcount.skip.patterns", false)) {
				Path[] patternsFiles = new Path[0];
				try {
					patternsFiles = DistributedCache.getLocalCacheFiles(conf);
				}catch (IOException ioe) {
					System.err.println("Caught : "+ StringUtils.stringifyException(ioe));
				}
				for (Path patternsFile: patternsFiles)
					parseSkipFile(patternsFile);
			}
		}
		public void cleanup(Context context) {
			patternsToSkip.clear();
		}
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = caseSensitive ? value.toString() : value.toString().toLowerCase();
			
			for (String pattern : patternsToSkip)
				line = line.replaceAll(pattern, "");
			
			StringTokenizer tokenizer = new StringTokenizer(line);
			while(tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				context.write(word, one);
				context.getCounter(Counters.INPUT_WORDS).increment(1);
				counter ++;
			}
			
		//context.write(new Text("total-->"), new IntWritable((int)context.getCounter(Counters.INPUT_WORDS).getValue());
		context.write(new Text("total-->"), new LongWritable(counter));
		
	if((++numRecords % 100) == 0)
		context.setStatus(String.valueOf(counter));
				////context.setStatus("Finished Processing " + numRecords + " records from the input file : "+ inputFile );
		}
	}
	public static class Reduce extends Reducer<Text, LongWritable, Text, LongWritable>{
		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException{
			int sum = 0;
			for (LongWritable value: values) {
				sum += value.get();
			}
			context.write(key, new LongWritable(sum));
		}
	}
	
	
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Job job = Job.getInstance(getConf(), "WordCount-2SkipCase");
		job.setJarByClass(WordCount2.class);
		
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		List<String> other_args = new ArrayList<String>();
		for (int i=0; i< args.length; ++i) {
			if ("-skip".equals(args[i])) {
				DistributedCache.addCacheFile(new Path(args[++i]).toUri(), job.getConfiguration());
				job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
			}else if ("-icase".equals(args[i])) {
				job.getConfiguration().setBoolean("wordcount.case.sensitive", false);
			}
			else other_args.add(args[i]);
		}
		FileInputFormat.setInputPaths(job,  new Path(other_args.get(0)));
		FileOutputFormat.setOutputPath(job, new Path(other_args.get(1)));
		
		return job.waitForCompletion(true) ? 0 : 1;
		
	}
	public static void main(String[] args) throws Exception{
		System.exit(ToolRunner.run(new Configuration(), new WordCount2(), args));
	}
}
