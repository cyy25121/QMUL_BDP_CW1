import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.text.*;
import java.util.Date;
import java.net.URI;
import java.util.Hashtable;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.Integer;

public class TweetsHashtagLookupNFilterMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final IntWritable one = new IntWritable(1);
    private Text THashtag = new Text();
	private SimpleDateFormat ft = new SimpleDateFormat ("yyyyMMdd");
	private String pattern1 = "(?<epochtime>\\d+);(\\d+);(?<tweets>.+);(.+)";
	private String pattern2 = "(?<hashtag>#[\\w]+)+";
	private String pattern3 = "^(go|team)(?<country>[a-z]+)";
	private Pattern r1 = Pattern.compile(pattern1);
	private Pattern r2 = Pattern.compile(pattern2);
	private Pattern r3 = Pattern.compile(pattern3, Pattern.CASE_INSENSITIVE);
	private Hashtable<String, String> country;

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {


  	  	Matcher m1 = r1.matcher(value.toString());
  	  	if(m1.find()){
			Matcher m2 = r2.matcher(m1.group("tweets"));
			while(m2.find()){
				String cty = country.get(m2.group("hashtag").toLowerCase().replaceAll("#", ""));
				if(cty != null){
					THashtag.set(cty);
					context.write(THashtag, one);
				}
				else{
					// go/team filter
					Matcher m3 = r3.matcher(m2.group("hashtag").toLowerCase().replaceAll("#", ""));
					if(m3.find()){
						// hashtable look up
						cty = country.get(m3.group("country"));
						if(cty != null){
							THashtag.set(cty);
							context.write(THashtag, one);
						}
					}
				}
			}
  	  	}
	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException{

		country = new Hashtable<String, String>();

		URI fileUri = context.getCacheFiles()[0];

		FileSystem fs = FileSystem.get(context.getConfiguration());
		FSDataInputStream in = fs.open(new Path(fileUri));

		BufferedReader br = new BufferedReader(new InputStreamReader(in));

		String line = null;
		try{
			br.readLine();
			while((line = br.readLine()) != null){
				context.getCounter(CustomCounters.NUM_COUNTRIES).increment(1);

				String [] fields = line.split(",");
				if (fields.length == 4){
					country.put(fields[1].toLowerCase(), fields[0]);
					country.put(fields[2].toLowerCase(), fields[0]);
					country.put(fields[3].toLowerCase(), fields[0]);
					country.put(fields[0].replaceAll("[,.&() \"]", "").toLowerCase(), fields[0]);
				}
			}
		} catch (IOException e1){
		}

		super.setup(context);
	}

}
