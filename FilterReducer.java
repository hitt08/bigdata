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

//Reduce class to find latest revision of Articles for the specified input timestamp 
public class FilterReducer extends Reducer<LongWritable, Text, Text, Text> {
	private Text _value = new Text();
	private DateFormat ISO_8601df;
	private Text _key = new Text();

	//Input Key -> Article ID
	//Input Value -> Article_Date + Article_Title + Main	
	public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		ISO_8601df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
		Date max_date = null;
		String temp_val="";
		String final_val="MAIN ";
		Set<String> main_set;
		
		for (Iterator<Text> it = values.iterator(); it.hasNext();) {
			Text temp_text = it.next(); //Article Value
			String[] temp_line = temp_text.toString().split("\n");	//Split value by "\n" for get different attributes of an article
			Date temp_date;
			try {
				temp_date = ISO_8601df.parse(temp_line[0]); //temp_line[0] is the timestamp
				
				//Checking Maximum Revision of an article
				if (max_date == null || max_date.before(temp_date)) {
					max_date = temp_date;
					_key.set(temp_line[1]);	//Article Title
					temp_val=temp_line[2];	//MAIN (outlinks)
				}
			} catch (ParseException e) {
				//Invalid Timestamp reject record
				e.printStackTrace();
			}
		}
		
		//Keeping Unique values in Main and removing self loop
		temp_val=temp_val.replaceAll("\t", " ");	//Replacing all tabs with spaces in outlink articles
		main_set = new HashSet<String>(Arrays.asList(temp_val.split(" ")));	//Includin all outlink articles in Hash Set to remove any duplicates

		main_set.remove(_key.toString());	//Removing self loop
		main_set.remove("MAIN");			//Removing tag "MAIN"
		final_val+=String.join(" ",main_set.toArray(new String[0]));	//Rewrite unique outlink article titles without self in MAIN tag	
		_value.set(final_val);		

		//Output Key-> Article Title
		//Output value-> Main
		context.write(_key, _value);
	}
}
