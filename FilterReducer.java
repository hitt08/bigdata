package mapreduce;

import java.io.IOException;
import java.util.*;
import java.text.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class FilterReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
	private int numRecords = 0;
	
	//key-> GroupID
	//value-> Article ID + Article Title + Main
	public void reduce(LongWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		String dummyRecord = "D\n"; // [D\n Article_Title Page_Rank #ofOutlinks\n]
		String _valueList = "";
		//Dummy Record Creation
		for (Iterator<Text> it = values.iterator(); it.hasNext();) {
			Text temp_text = it.next(); //Article Value (without date)
			String[] temp_line = temp_text.toString().split("\n");
			String [] temp_main = temp_line[2].split(" "); //MAIN of current article
			int temp_outLinks = temp_main.length - 1;
			double temp_pagerank = 0.15 + (0.85 * temp_outLinks); //Initializing PageRank of current article
			dummyRecord += temp_line[1] +" "+ temp_pagerank +" "+temp_outLinks+"\n";
			//key -> GroupID
			//value-> Article ID + Article Title + Main + PageRank + #Outlinks
			_valueList += temp_text.toString() + "\n" + temp_pagerank + "\n" + temp_outLinks;
		}
		
		context.write(key, new Text(dummyRecord + _valueList));
	}
}
