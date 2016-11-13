import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.lang.Math;

public class TweetsUnnormalMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final IntWritable one = new IntWritable(1);
    private Text tweets = new Text();
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String pattern = "(\\d+);(\\d+);(?<tweets>.+);(.+)";

		Pattern r = Pattern.compile(pattern);
  	  	Matcher m = r.matcher(value.toString());
  	  	if(m.find()){
			if(m.group("tweets").length() > 140){
				tweets.set(m.group("tweets"));
				context.write(tweets, one);
			}

		}
	}
}
