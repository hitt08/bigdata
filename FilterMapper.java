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

import mapreduce.Mapper.Counters;
import utils.ISO8601;

public class FilterMapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, LongWritable, Text> {

	private LongWritable _key = new LongWritable();
	private Text _value = new Text();
	private Date date_timestamp;
	private long numRecords = 0;
	private DateFormat ISO_8601df;
	private static HashMap<String,Long> groupIdMap; //ArticleTitle -> GroupID
	private static long currentGroupId;

	public void setup(Context context) {
		Configuration conf = context.getConfiguration();
		ISO_8601df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
		groupIdMap=new HashMap();
		currentGroupId=0;
		try {
			date_timestamp = ISO_8601df.parse(conf.get("pagerank.date", ""));
			System.out.println("ConfDate : " + conf.get("pagerank.date") + "Timestamp: " + date_timestamp.toString());
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
		
		int i=0;
		while (tokenizer.hasMoreTokens()) {
			String line = tokenizer.nextToken();
				// Date
			if(i==0) {
				try {
					Date article_date = ISO_8601df.parse(line);

					if (article_date.after(date_timestamp)) {
						
						if((++numRecords % 100) == 0) {
							System.out.println("ArticleDate: " + article_date.toString() + " Timestamp: " + date_timestamp.toString());
							}
						skip_record = true;
					}

				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					System.out.println("Exception in the map!\n"+ e.toString());
					skip_record = true;
				}
			}else if(i == 1) {
				article_title = line;
				
				if(!groupIdMap.containsKey(line)){
					groupIdMap.put(line, ++currentGroupId);
					article_groupId = currentGroupId;
				}else {
					article_groupId = groupIdMap.get(line);
				}			
			}
			else if (i==2){
				String [] main = line.split(" ");
				for(int x=1; x< main.length; x++) {
					links.add(main[x]);
				}
			}
			i++;
		}

		if (!skip_record) {
			
			for(String s : links) {
				if(!groupIdMap.containsKey(s)) {
					groupIdMap.put(s, article_groupId);
				}
			}
		if(numRecords % 100 == 0)
			System.out.println("Article_groupID: " + article_groupId + " Article_Title: " + article_title);
			
			String temp = value.toString();
			temp = String.valueOf(article_groupId) + "\n" + temp;

			//key -> Article ID
			//Value -> Group ID + Article_Date + Article_Title + Main
			
			context.write(key, new Text(temp));
		}
	}
	
}