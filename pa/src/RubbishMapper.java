import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.lang.Math;

public class RubbishMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
    private final IntWritable one = new IntWritable(1);
	private final IntWritable zero = new IntWritable(0);
    private Text t = new Text();
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String pattern = "(\\d+);(\\d+);(?<tweets>.+);(.+)";

		Pattern r = Pattern.compile(pattern);
  	  	Matcher m = r.matcher(value.toString());
  	  	if(m.find()){
			//t.set(value.toString());
			context.write(one, one);
		}
		else{
			//t.set(value.toString());
			context.write(zero, one);

		}
	}
}
