// Kexin Liu
// Sep 11, 2024
// Create a RedditAverage class to calculate score in each subreddit 

import org.json.JSONObject;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class RedditAverage extends Configured implements Tool {
	public static class TokenizerMapper
	extends Mapper<LongWritable, Text, Text, LongPairWritable>{
		private Text subreddit = new Text();
		private LongPairWritable pair = new LongPairWritable();

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			try {
				JSONObject record = new JSONObject(value.toString());
				String subreddit_name = (String) record.get("subreddit");
				int score = (Integer) record.get("score");
				subreddit.set(subreddit_name);
				pair.set(1, score); 
				context.write(subreddit, pair);
			} catch (Exception e) {
				System.err.println("Error: " + e.getMessage());
			}
		}

	}


	public static class Combiner
	extends Reducer<Text, LongPairWritable, Text, LongPairWritable> {
		private LongPairWritable shuffled_pair = new LongPairWritable();

		@Override
		public void reduce(Text key, Iterable<LongPairWritable> values, Context context) throws IOException, InterruptedException {
			long comment_count = 0;
			long total_score = 0;

			for (LongPairWritable val : values) {
				comment_count += val.get_0();
				total_score += val.get_1();
			} 
			shuffled_pair.set(comment_count, total_score);
			context.write(key, shuffled_pair);
		}
	}

	public static class DoubleSumReducer
	extends Reducer<Text, LongPairWritable, Text, DoubleWritable> {
		private DoubleWritable average = new DoubleWritable();

		@Override
		public void reduce(Text key, Iterable<LongPairWritable> values, Context context) throws IOException, InterruptedException {
			long total_score = 0;
			long comment_count = 0;
			for (LongPairWritable val : values) {
				comment_count += val.get_0();
				total_score += val.get_1();
			}
			average.set((double) total_score/comment_count);
			context.write(key, average);
		}
	}
	

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new RedditAverage(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf, "reddit average");
		job.setJarByClass(RedditAverage.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(Combiner.class);
		job.setReducerClass(DoubleSumReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongPairWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		job.setOutputFormatClass(TextOutputFormat.class);
		TextInputFormat.addInputPath(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true) ? 0 : 1;
	}
}
