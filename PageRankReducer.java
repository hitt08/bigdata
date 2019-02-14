package mapreduce;

import java.io.IOException;
import java.util.*;
import java.text.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import ch.epfl.lamp.fjbg.JConstantPool.IntegerEntry;

public class PageRankReducer extends Reducer<Text, Text, Text, Text> {
	private Text _value = new Text();
	private Text _key = new Text();
	private double sum = 0;
	private String Main = "";
	private double previousScore = 1;
	private String separator = "--!--";
	
	//key -> Title
	//value-> Score--!--Main
	
	//OR
	
	//key-> Title
	//Value -> Score
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		for (Iterator<Text> it = values.iterator(); it.hasNext();) {
			Text value_text = it.next(); //Article Value
			String [] valuesplit = value_text.toString().split(separator);
			if(valuesplit.length > 1) {
				Main = valuesplit[1];
				previousScore = Double.valueOf(valuesplit[0]);
			}
			sum += Double.valueOf(valuesplit[0]);
		}
		
		if(context.getConfiguration().getBoolean("pagerank.finalIteration", false)) {
			//Final Round
			//Key -> Title
			//Value -> CScore
			_value.set(String.valueOf(sum));
		}else {
		
		//Key -> Title
		//Value -> CScore PScore\nMain
		_value.set(String.valueOf(sum) + " " + String.valueOf(previousScore) + separator + Main);
		}
		context.write(key, _value);
	}
}
