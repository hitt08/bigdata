package mapreduce;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

//Map class to get Articles older than specified input timestamp 
public class FilterMapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, LongWritable, Text> {

	private Date date_timestamp;
	private DateFormat ISO_8601df;

	public void setup(Context context) {
		Configuration conf = context.getConfiguration();
		ISO_8601df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");	//ISO8601df Date format
		try {
			date_timestamp = ISO_8601df.parse(conf.get("pagerank.date", ""));	//Parse input timestamp from job configuration
		} catch (ParseException e) {
			// Invalid Date
			e.printStackTrace();
			System.out.println("Exception in the setup!\n" + e.toString());
			System.exit(1);	//Exit the job
		}
	}

	//Input Key-> Article ID
	//Input Value-> Article_date + Title + Main
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		StringTokenizer tokenizer = new StringTokenizer(value.toString(), "\n");	//Split value by "\n" for get different attributes of an article 
		boolean skip_record = false;
		
		//while (tokenizer.hasMoreTokens()) {		//Loop
			String line = tokenizer.nextToken();	//Get Article Timestamp as the first token
			//Date formatting
				try {
					Date article_date = ISO_8601df.parse(line);		//Parse article timestamp

					if (article_date.after(date_timestamp)) {		//Check if article timestamp is newer than input timestamp 
						skip_record = true;		//Mark this record for skip
					}

				} catch (ParseException e) {
					//Invalid Date
					e.printStackTrace();
					System.out.println("Exception in the map!\n"+ e.toString());
					skip_record = true;		//Skip article revision with invalid timestamp
				}

		//	break;
	//	}
			

		if (!skip_record) {	//Write the article record into context if matches the date condition
			//Ouput key -> Article ID
			//Output Value -> Article_Date + Article_Title + Main
			context.write(key, value);		//Same as input
		}
	}
	
}