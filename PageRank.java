package mapreduce;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PageRank extends Configured implements Tool{
	
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		//Filtering Job
		Job filter_job = Job.getInstance(getConf(), "Filter_PageRank");
		int file_index = 0;
		boolean jobDone = false;
		filter_job.setJarByClass(PageRank.class);
		
		filter_job.setMapperClass(FilterMapper.class);
		//filter_job.setPartitionerClass(MyPartitioner.class);
		filter_job.setMapOutputKeyClass(LongWritable.class);
		filter_job.setMapOutputValueClass(Text.class);
		
		filter_job.setReducerClass(FilterReducer.class);
		//filter_job.setNumReduceTasks(4);
		
		filter_job.setInputFormatClass(FilterInputFormat.class);
		filter_job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(filter_job, new Path(args[0]));
		FileOutputFormat.setOutputPath(filter_job, new Path(args[1] + (file_index)));

		//Date
		filter_job.getConfiguration().set("pagerank.date", args[3]);
		
		
		if(filter_job.waitForCompletion(true)) {
			
			//Number of Iterations
		
			int _it = 0;
			do {
			Job main_job = Job.getInstance(getConf(), "Main_PageRank" + _it);
			main_job.setJarByClass(PageRank.class);
			
			main_job.setMapperClass(PageRankMapper.class);
			main_job.setMapOutputKeyClass(Text.class);
			main_job.setMapOutputValueClass(Text.class);
			
			main_job.setReducerClass(PageRankReducer.class);
			//main_job.setNumReduceTasks(4);
			
			main_job.setInputFormatClass(TextInputFormat.class);
			main_job.setOutputFormatClass(TextOutputFormat.class);
			
				FileInputFormat.setInputPaths(main_job, new Path(args[1] + file_index));
				
				if(_it + 1 == Integer.valueOf(args[2])) {
					main_job.getConfiguration().setBoolean("pagerank.finalIteration", true);
					FileOutputFormat.setOutputPath(main_job, new Path(args[1]));
				}else {
					main_job.getConfiguration().setBoolean("pagerank.finalIteration", false);
					FileOutputFormat.setOutputPath(main_job, new Path(args[1] + (++file_index)));
				}
			
				_it++;
			jobDone = main_job.waitForCompletion(true);
			}while( jobDone && _it < Integer.valueOf(args[2]));
		}

		return jobDone ? 0 : 1;
		
	}
	public static void main(String[] args) throws Exception{
		System.exit(ToolRunner.run(new Configuration(), new PageRank(), args));
	}
	

}



//"Bad_Day"       6256952.803452841