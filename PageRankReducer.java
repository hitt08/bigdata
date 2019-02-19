package mapreduce;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PageRankReducer extends Reducer<Text, Text, Text, Text> {
	private Text _value = new Text();
	private double sum = 0;
	private String Main = "";
	private String separator = "--!--";
	
	/*Input Key -> Title
	 * 
	 *Input Value-> Score--!--Main		
	 *OR
	 *Input Value -> Score		(For articles with no outlinks)*/
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		sum=0;
		Main="";
		for (Iterator<Text> it = values.iterator(); it.hasNext();) {
			
			Text value_text = it.next(); //Article Value
			String [] valuesplit = value_text.toString().split(separator); //Separating Score and MAIN
			if(valuesplit.length > 1) {	//Article with Outlinks
				Main = valuesplit[1];
			}
			sum += Double.valueOf(valuesplit[0]);	//Sum of all scores contributed by other articles
		}
		sum=0.15 + (0.85 * sum);	//Page rank Formula
		
		//Check for final round
		if(context.getConfiguration().getBoolean("pagerank.finalIteration", false)) {
			//Final Round
			//Output Key -> Title
			//Output Value -> CScore
			_value.set(String.valueOf(sum));
		}else {
			//For next round
			//Output Key -> Title
			//Output Value -> CScore--!--Main
			_value.set(String.valueOf(sum) + separator + Main);
		}
		context.write(key, _value);
	}
}
