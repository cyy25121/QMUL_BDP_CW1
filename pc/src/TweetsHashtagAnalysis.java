import java.util.Arrays;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

enum CustomCounters {NUM_COUNTRIES}

public class TweetsHashtagAnalysis {
	/*
	 *	Hashtag count
	 *	mapper(Text, Int)
	 *	reducer(Text, int)
	 */
	public static void runJob1(String[] input, String output) throws Exception {

		Configuration conf = new Configuration();

		Job job = new Job(conf);
		job.setJarByClass(TweetsHashtagAnalysis.class);
		job.setMapperClass(TweetsHashtagCountMapper.class);
		job.setReducerClass(IntSumReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		Path outputPath = new Path(output);
		FileInputFormat.setInputPaths(job, StringUtils.join(input, ","));
		FileOutputFormat.setOutputPath(job, outputPath);
		outputPath.getFileSystem(conf).delete(outputPath,true);
		job.waitForCompletion(true);
	}
	/*
	 *	Hashtag lookup
	 *	mapper(Text, Int)
	 *	reducer(Text, Int)
	 */
	public static void runJob2(String[] input, String output) throws Exception {

		Configuration conf = new Configuration();

		Job job = new Job(conf);
		job.setJarByClass(TweetsHashtagAnalysis.class);
		job.setMapperClass(TweetsHashtagLookupMapper.class);
		job.setReducerClass(IntSumReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.addCacheFile(new Path("input/countrycode.csv").toUri());
		Path outputPath = new Path(output);
		FileInputFormat.setInputPaths(job, StringUtils.join(input, ","));
		FileOutputFormat.setOutputPath(job, outputPath);
		outputPath.getFileSystem(conf).delete(outputPath,true);
		job.waitForCompletion(true);
	}

	/*
	 *	Hashtag lookup countrycode portion
	 *	mapper(Text, Pair(Int, Int))
	 *	reducer(Text, Text)
	 */
	public static void runJob3(String[] input, String output) throws Exception {

		Configuration conf = new Configuration();

		Job job = new Job(conf);
		job.setJarByClass(TweetsHashtagAnalysis.class);
		job.setMapperClass(TweetsHashtagLookupCodePortionMapper.class);
		job.setReducerClass(IntIntSumReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntIntPair.class);
		job.addCacheFile(new Path("input/countrycode.csv").toUri());
		Path outputPath = new Path(output);
		FileInputFormat.setInputPaths(job, StringUtils.join(input, ","));
		FileOutputFormat.setOutputPath(job, outputPath);
		outputPath.getFileSystem(conf).delete(outputPath,true);
		job.waitForCompletion(true);
	}

	/*
	 *	Hashtag lookup plus filter(go, team)
	 *	mapper(Text, Int)
	 *	Reducer(Text, Int)
	 */
	public static void runJob4(String[] input, String output) throws Exception {

		Configuration conf = new Configuration();

		Job job = new Job(conf);
		job.setJarByClass(TweetsHashtagAnalysis.class);
		job.setMapperClass(TweetsHashtagLookupNFilterMapper.class);
		job.setReducerClass(IntSumReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.addCacheFile(new Path("input/countrycode.csv").toUri());
		Path outputPath = new Path(output);
		FileInputFormat.setInputPaths(job, StringUtils.join(input, ","));
		FileOutputFormat.setOutputPath(job, outputPath);
		outputPath.getFileSystem(conf).delete(outputPath,true);
		job.waitForCompletion(true);
	}

	public static void main(String[] args) throws Exception {
		runJob1(Arrays.copyOfRange(args, 0, args.length-4), args[args.length-4]);
		runJob2(Arrays.copyOfRange(args, 0, args.length-4), args[args.length-3]);
		runJob3(Arrays.copyOfRange(args, 0, args.length-4), args[args.length-2]);
		runJob4(Arrays.copyOfRange(args, 0, args.length-4), args[args.length-1]);
	}

}
