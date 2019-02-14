package mapreduce;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;

import utils.ISO8601;

public class FilterMapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, LongWritable, Text> {

	private LongWritable _key = new LongWritable();
	private Text _value = new Text();
	private Date date_timestamp;
	private long numRecords = 0;
	private DateFormat ISO_8601df;

	public void setup(Context context) {
		Configuration conf = context.getConfiguration();
		ISO_8601df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
		try {
			date_timestamp = ISO_8601df.parse(conf.get("pagerank.date", ""));
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("Exception in the setup!\n" + e.toString());
		}
	}

	//key-> Article ID
	//value-> Article_date + Title + Main
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		StringTokenizer tokenizer = new StringTokenizer(value.toString(), "\n");
		boolean skip_record = false;
		Set<String> links = new HashSet<String>();
		long article_groupId = 0;
		String article_title = ""; //Get Article Title
		
		while (tokenizer.hasMoreTokens()) {
			String line = tokenizer.nextToken();
			//Date formatting
				try {
					Date article_date = ISO_8601df.parse(line);

					if (article_date.after(date_timestamp)) {
						skip_record = true;
					}

				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					System.out.println("Exception in the map!\n"+ e.toString());
					skip_record = true;
				}

			break;
		}
			

		if (!skip_record) {

			//key -> Article ID
			//Value -> Article_Date + Article_Title + Main
			
			context.write(key, value);
		}
	}
	
}