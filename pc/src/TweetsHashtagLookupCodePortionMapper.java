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

public class TweetsHashtagLookupCodePortionMapper extends Mapper<Object, Text, Text, IntIntPair> {
    private final IntWritable one = new IntWritable(1);
	private final IntWritable zero = new IntWritable(0);
	private final IntIntPair p = new IntIntPair();
    private Text THashtag = new Text();
	private String pattern1 = "(?<epochtime>\\d+);(\\d+);(?<tweets>.+);(.+)";
	private String pattern2 = "(?<hashtag>#[\\w]+)+";
	private Pattern r1 = Pattern.compile(pattern1);
	private Pattern r2 = Pattern.compile(pattern2);
	private Hashtable<String, String> country;
	private Hashtable<String, String> country_code_only;

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

  	  	Matcher m1 = r1.matcher(value.toString());
  	  	if(m1.find()){
			Matcher m2 = r2.matcher(m1.group("tweets"));
			while(m2.find()){
				String cty = country.get(m2.group("hashtag").toLowerCase().replaceAll("#", ""));
				String cty_code_only = country_code_only.get(m2.group("hashtag").toLowerCase().replaceAll("#", ""));
				if(cty != null){
					THashtag.set(cty);
					if(cty_code_only != null){
						p.set(one, one);
					}
					else{
						p.set(one, zero);
					}
					context.write(THashtag, p);
				}
			}
  	  	}
	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException{

		country = new Hashtable<String, String>();
		country_code_only = new Hashtable<String, String>();

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

					country_code_only.put(fields[1].toLowerCase(), fields[0]);
					country_code_only.put(fields[2].toLowerCase(), fields[0]);
					country_code_only.put(fields[3].toLowerCase(), fields[0]);
				}
			}
		} catch (IOException e1){
		}

		super.setup(context);
	}

}
