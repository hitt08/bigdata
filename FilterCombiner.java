package mapreduce;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import com.google.common.collect.ArrayTable;

public class FilterCombiner extends Reducer<LongWritable, Text, LongWritable, Text> {
	private Text _value = new Text();
	private DateFormat ISO_8601df;
	private int numRecords = 0;
	private LongWritable _key = new LongWritable();

	//key -> Article ID
	//Value -> Group ID + Article_Date + Article_Title + Main
	
	public void reduce(LongWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		ISO_8601df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
		Date max_date = null;
		
		for (Iterator<Text> it = values.iterator(); it.hasNext();) {
			Text temp_text = it.next(); //GroupID + Article Value
			String[] temp_line = temp_text.toString().split("\n");
			Date temp_date;
			try {
				temp_date = ISO_8601df.parse(temp_line[1]); //temp_line is the date
				
				//Checking Maximum Revision of an article
				if (max_date == null || max_date.before(temp_date)) {
					max_date = temp_date;
					_key.set(Integer.valueOf(temp_line[0]));
					temp_line[0] = String.valueOf(key.get()); //Replace group ID with article ID in values
					temp_line[1] = ""; //Removing date
					_value.set(String.join("\n", temp_line).replace("\n\n", "\n"));
					
				}
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		if(++numRecords % 100 == 0) {
			System.out.println("GroupID: " + _key.toString() + " Value: " + _value.toString().split("\n")[1]);
		}
		
		//Key-> Group ID
		//value-> Article ID + Article Title + Main
		context.write(_key, _value);
	}
}
