package mapreduce;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import com.google.common.collect.ArrayTable;

public class FilterReducer extends Reducer<LongWritable, Text, Text, Text> {
	private Text _value = new Text();
	private DateFormat ISO_8601df;
	private int numRecords = 0;
	private Text _key = new Text();

	//key -> Article ID
	//Value -> Article_Date + Article_Title + Main
	
	public void reduce(LongWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		ISO_8601df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
		Date max_date = null;
		String temp_val="";
		String final_val="MAIN ";
		Set<String> main_set;
		
		for (Iterator<Text> it = values.iterator(); it.hasNext();) {
			Text temp_text = it.next(); //Article Value
			String[] temp_line = temp_text.toString().split("\n");
			Date temp_date;
			try {
				temp_date = ISO_8601df.parse(temp_line[0]); //temp_line is the date
				
				//Checking Maximum Revision of an article
				if (max_date == null || max_date.before(temp_date)) {
					max_date = temp_date;
					_key.set(temp_line[1]);
					//_value.set(temp_line[2]);
					temp_val=temp_line[2];
				}
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		//Keeping Unique values in Main
		temp_val=temp_val.replaceAll("\t", " ");
		main_set = new HashSet<String>(Arrays.asList(temp_val.split(" ")));
		

		main_set.remove(_key.toString());
		main_set.remove("MAIN");
		final_val+=String.join(" ",main_set.toArray(new String[0]));	
		_value.set(final_val);		
		//Key-> Article Title
		//value-> Main
		context.write(_key, _value);
	}
}
