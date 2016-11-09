import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IntIntSumReducer extends Reducer<Text, IntIntPair, Text, Text> {
    private Text result = new Text();
    public void reduce(Text key, Iterable<IntIntPair> values, Context context)
    throws IOException, InterruptedException {

        int sum = 0;
		int sum_code_only = 0;
        for (IntIntPair value : values) {
			sum += value.getFirst().get();
			sum_code_only += value.getSecond().get();
        }
        result.set(sum_code_only + "\t" + sum + "\t" + (double)sum_code_only/sum);
		context.write(key, result);
    }
}
