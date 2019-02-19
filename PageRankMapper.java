package mapreduce;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

//Map Class to contribute pagerank score to outlinks 
public class PageRankMapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text>{
	private Text _value = new Text();
	private Text _key = new Text();
	private String separator = "--!--";		//Separator for value fields

	/*Input Key -> Line# (Default from Text Input Formatter)
	*
	* Input Value -> ArticleTitle\tMain		(From Filter Reducer)
	* OR
	* Input Value -> ArticleTitle\tCScore--!--Main	(From Previous Round)*/
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {		
		String []valuestr = value.toString().split("\t");	//Split Actual Key Value with tab
		String secondLine = valuestr[1]; //Main OR CScore PScore--!--Main
		
		if(secondLine.charAt(0) == 'M') {	//If Input from Filter Reducer
			//Initializing
			pageRankToOutlink(1, secondLine, context);		//Contributing Intial score of 1 to outlinks
			_value.set("0" + separator + secondLine);		//Keeping self score as 0 with all outlink titles			
		}else {
			//Input from Previous Round
			String [] valuesplit = secondLine.split(separator);  //CScore--!--Main
			double currentScore = Double.valueOf(valuesplit[0]);
			
			if(valuesplit.length > 1) {		//If not target article i.e. Article with no outlinks
				pageRankToOutlink(currentScore, valuesplit[1], context);	//Contributing pagerank score from previous round to outlinks
				_value.set("0" + separator + valuesplit[1]);	//Keeping self score as 0 with all outlink titles
			}
			else
				_value.set("0");	//Article with no outlinks
			
		}
		//Output key -> Title
		//Output Value -> CScore\nMain
		_key.set(valuestr[0]);
		context.write(_key, _value);
	}
	
	//Contribute self pagerank score with outlinks
	public void pageRankToOutlink(double currentScore, String main, Context context) throws IOException, InterruptedException{
		String[] outlink_articles = main.split(" "); //outlink articles in MAIN
		
		double temp_outLinks = outlink_articles.length - 1;	//All except "MAIN" tag
		double temp_pagerank = currentScore/temp_outLinks;	//Score to contribute
		
		//Sending self score to outlinks
		for (int i=1; i< outlink_articles.length; i++) {
			_key.set(outlink_articles[i]);		//Outlink Article Title
			_value.set(String.valueOf(temp_pagerank));
			context.write(_key, _value);
		}
	}
}
