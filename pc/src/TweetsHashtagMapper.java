import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.text.*;
import java.util.Date;

public class TweetsHashtagMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final IntWritable one = new IntWritable(1);
    private Text THashtag = new Text();
	private SimpleDateFormat ft = new SimpleDateFormat ("yyyyMMdd");
	private String pattern1 = "(?<epochtime>\\d+);(\\d+);097;(.+)";
	private String pattern2 = "(?<hashtag>#[\\w]+)+";
	private Pattern r1 = Pattern.compile(pattern1);
	private Pattern r2 = Pattern.compile(pattern2);
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {


  	  	Matcher m1 = r1.matcher(value.toString());
  	  	if(m1.find()){
			Matcher m2 = r2.matcher(m1.group("tweets"));
			while(m2.find()){
				THashtag.set(m2.group("hashtag"));
				if(m2.group("hashtag").equals("#0")){
					System.out.println(m1.group("tweets"));
				}
				context.write(THashtag, one);
			}
  	  	}
	}
}
