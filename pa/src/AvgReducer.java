import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AvgReducer extends Reducer<IntWritable, IntWritable, Text, DoubleWritable> {
    private DoubleWritable result = new DoubleWritable();
	private Text avg = new Text("Avg:");
    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
    throws IOException, InterruptedException {

        long sum = 0;
		int count = 0;
        for (IntWritable value : values) {
			sum += value.get();
			count++;
        }
		System.out.println("[DEBUG] " + sum + ", " + count);
		result.set(sum/(double)count);
		context.write(avg, result);
    }
}
