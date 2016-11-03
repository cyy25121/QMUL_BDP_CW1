import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TweetsLengthMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
    private final IntWritable one = new IntWritable(1);
    private final IntWritable nine = new IntWritable(999);
    private final IntWritable ot = new IntWritable(1000);
    private IntWritable tweets = new IntWritable();
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        //StringTokenizer itr = new StringTokenizer(value.toString(), "-- \t\n\r\f,.:;?![]'\"");
        //while (itr.hasMoreTokens()) {
        //    data.set(itr.nextToken().toLowerCase());
        //    context.write(data, one);
        //}
		String pattern = "(\\d+);(\\d+);(?<tweets>.+);(.+)";

		Pattern r = Pattern.compile(pattern);
  	  	Matcher m = r.matcher(value.toString());
  	  	if(m.find()){
			tweets.set((m.group("tweets").length()) / 5 + 1);
			context.write(tweets, one);
  	  	}
        else{
			context.write(nine, one);
        }
		context.write(ot, one);

	}
}
