import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.lang.Math;

public class TweetsLengthMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
    private final IntWritable one = new IntWritable(1);
    private IntWritable tweets = new IntWritable();
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String pattern = "(\\d+);(\\d+);(?<tweets>.+);(.+)";

		Pattern r = Pattern.compile(pattern);
  	  	Matcher m = r.matcher(value.toString());
  	  	if(m.find()){
			tweets.set((int)Math.ceil(m.group("tweets").length()/5.0) - 1);
			context.write(tweets, one);
		}
	}
}
