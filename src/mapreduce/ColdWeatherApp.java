package mapreduce;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 数据文件内容：
20170601070801+26
20170601070802+22
20170601070803+20
20170602080945+19
20180101120101-10
20180101220101-19
20190912050102+13
20190912053003+11
20190912133003+19
 */

//求最低温度
public class ColdWeatherApp {
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://master:9000");
		FileSystem hdfs = FileSystem.get(conf);
		Path input = new Path("/local/weather.txt");
		Path out = new Path("/output/weatherout");
		
		if (hdfs.exists(out)) {
			hdfs.delete(out, true);
		}
		
		Job job = Job.getInstance(conf, "hotWeahter");
		job.setJarByClass(ColdWeatherApp.class);
		job.setMapperClass(WeatherMapper.class);
		job.setReducerClass(WeatherReducer.class);
		job.setPartitionerClass(WeatherPartition.class);
		job.setNumReduceTasks(2);
		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, out);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		job.waitForCompletion(true);
		System.out.println("done");
	}

	// Mapper
	public static class WeatherMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, IntWritable, IntWritable>.Context context)
				throws IOException, InterruptedException {
			String data = value.toString();
			int year = Integer.parseInt(data.substring(0, 4));
			int hot = 0;
			char flag = data.charAt(14);
			if (flag == '+') {
				hot = Integer.parseInt(data.substring(15, 17));
			} else {
				hot = Integer.parseInt(data.substring(14, 17));
			}
			context.write(new IntWritable(year), new IntWritable(hot));
		}
	}

	// Reducer
	public static class WeatherReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
		@Override
		protected void reduce(IntWritable key, Iterable<IntWritable> values,
				Reducer<IntWritable, IntWritable, IntWritable, IntWritable>.Context context)
				throws IOException, InterruptedException {
			int min = Integer.MIN_VALUE;
			for (IntWritable value : values) {
				if (min > value.get()) {
					min = value.get();
				}
			}
			context.write(key, new IntWritable(min));
		}
	}
	
	public static class WeatherPartition extends Partitioner<IntWritable, IntWritable> {
		@Override
		public int getPartition(IntWritable key, IntWritable value, int numPartitions) {
			int year = key.get();
			if (year % 2 == 0) {
				return 1;
			}
			return 0;
		}
	}
}
