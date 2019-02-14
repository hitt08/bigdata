package mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class PageRankMapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text>{
	private Text _value = new Text();
	private Text _key = new Text();
	private String separator = "--!--";
	
	
	//Key -> Line#
	//Value -> ArticleTitle\tMain
	
	//OR
	
	//Key -> Line#
	//Value -> ArticleTitle\tCScore PScore--!--Main
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {		
		String []valuestr = value.toString().split("\t");
		String secondLine = valuestr[1]; //Main OR CScore PScore--!--Main
		
		if(secondLine.charAt(0) == 'M') {
			//Initializing
			
			pageRankToOutlink(1, secondLine, context);
			_value.set("1" + separator + secondLine);
			
		}else {
			//CScore PScore\nMain
			String [] valuesplit = secondLine.split(separator);
			double currentScore = Double.valueOf(valuesplit[0].split(" ")[0]);
			double previousScore = Double.valueOf(valuesplit[0].split(" ")[1]);	
		if(valuesplit.length > 1) {
			if(currentScore != previousScore) {
				pageRankToOutlink(currentScore, valuesplit[1], context);
			}
			_value.set(String.valueOf(currentScore) + separator + valuesplit[1]);
		}else _value.set(String.valueOf(currentScore));
		}
		//key -> Title
		//Value -> CScore\nMain
		_key.set(valuestr[0]);
		context.write(_key, _value);
	}
	
	public void pageRankToOutlink(double currentScore, String main, Context context) throws IOException, InterruptedException{
		String[] outlink_articles = main.split(" "); //outlink articles in MAIN
		
		int temp_outLinks = outlink_articles.length - 1;
		double temp_pagerank = 0.15 + (0.85 * (currentScore/temp_outLinks));
		
		//Sending self score to outlinks
		for (int i=1; i< outlink_articles.length; i++) {
			_key.set(outlink_articles[i]);
			_value.set(String.valueOf(temp_pagerank));
			context.write(_key, _value);
		}
	}
}
