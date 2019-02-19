package mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//Main Class
public class PageRank extends Configured implements Tool{
	
	@Override
	public int run(String[] args) throws Exception {
		
		//Filtering Job - Filter Articles Later to the specified timestamp, and find latest revision of Articles
		Job filter_job = Job.getInstance(getConf(), "Filter_PageRank");
		int file_index = 0;
		boolean jobDone = false;
		filter_job.setJarByClass(PageRank.class);
		
		filter_job.setMapperClass(FilterMapper.class);			//Map class for filter stage
		filter_job.setMapOutputKeyClass(LongWritable.class);	//Key as Article Id
		filter_job.setMapOutputValueClass(Text.class);			//Value as Article Timestamp, Title and Main 
		
		filter_job.setReducerClass(FilterReducer.class);		//Reducer class for filter stage
		
		filter_job.setInputFormatClass(FilterInputFormat.class);	//Custom Input Formatter for restricting only required data
		filter_job.setOutputFormatClass(TextOutputFormat.class);	//Default Text Output Formatter
		
		FileInputFormat.setInputPaths(filter_job, new Path(args[0]));	//Input File HDFS Path
		FileOutputFormat.setOutputPath(filter_job, new Path(args[1] + (file_index)));	//Output Directory Suffixed by index to represent the round (0th round for filter stage)

		//Set timestamp from input parameters to configuration
		filter_job.getConfiguration().set("pagerank.date", args[3]);
		
		//Check for Filter job completion
		if(filter_job.waitForCompletion(true)) {
			
			//Run Job N times : N->Number of Iterations as specified in input 
			int _it = 0;
			do {
				Job main_job = Job.getInstance(getConf(), "Main_PageRank" + _it); //Create new job for each iteration
				main_job.setJarByClass(PageRank.class);
				
				main_job.setMapperClass(PageRankMapper.class);		//Map class for contributing Page rank scores
				main_job.setMapOutputKeyClass(Text.class);			//Key as Article Title
				main_job.setMapOutputValueClass(Text.class);		//Value as Contributed Score and Main(For non-target articles)
				
				main_job.setReducerClass(PageRankReducer.class);	//Reduce class for summing Page rank scores
				
				main_job.setInputFormatClass(TextInputFormat.class);	//Default Text Input Formatter
				main_job.setOutputFormatClass(TextOutputFormat.class);	//Default Text Output Formatter
				
				FileInputFormat.setInputPaths(main_job, new Path(args[1] + file_index));	//Input Directory as Output of previous job
				
				//Check for Final Round
				if(_it + 1 == Integer.valueOf(args[2])) {
					//Final Round
					main_job.getConfiguration().setBoolean("pagerank.finalIteration", true);	//Set Final Round Boolean parameter to TRUE in job configuration
					FileOutputFormat.setOutputPath(main_job, new Path(args[1]));				//Output Directory as provided in input parameter
				}else {
					main_job.getConfiguration().setBoolean("pagerank.finalIteration", false);		//Set Final Round Boolean parameter to FAlSE in job configuration
					FileOutputFormat.setOutputPath(main_job, new Path(args[1] + (++file_index)));	//Output Directory Suffixed by index to represent the round number
				}
			
				_it++;	//Increment Round
			jobDone = main_job.waitForCompletion(true);		//Store completion status of job
			}while( jobDone && _it < Integer.valueOf(args[2]));		//Run next round if Job Not Failed and Not Final Round
		}

		return jobDone ? 0 : 1;		//Exit Status
		
	}
	public static void main(String[] args) throws Exception{
		System.exit(ToolRunner.run(new Configuration(), new PageRank(), args));
	}
	

}