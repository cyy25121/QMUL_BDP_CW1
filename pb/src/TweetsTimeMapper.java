import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.text.*;
import java.util.Date;

public class TweetsTimeMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final IntWritable one = new IntWritable(1);
    private Text Tdate = new Text();
	private SimpleDateFormat ft = new SimpleDateFormat ("yyyyMMdd");
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String pattern = "(?<epochtime>\\d+);(\\d+);(?<tweets>.+);(.+)";

		Pattern r = Pattern.compile(pattern);
  	  	Matcher m = r.matcher(value.toString());
  	  	if(m.find()){
			Date tweetsdate = new Date(Long.parseLong(m.group("epochtime")));
			Tdate.set(ft.format(tweetsdate));
			context.write(Tdate, one);
  	  	}
	}
}
