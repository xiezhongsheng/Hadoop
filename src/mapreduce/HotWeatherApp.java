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

public class HotWeatherApp {
	public static void main(String[] args) throws Exception {
		// 定义Configuration对象
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://master:9000");
		FileSystem hdfs = FileSystem.get(conf);
		Path input = new Path("/local/weather.txt");
		Path out = new Path("/output/weatherout");
		// 判断输出路径是不存在，如果存在，将其删除
		if (hdfs.exists(out)) {
			hdfs.delete(out, true);
		}
		// 声明一个任务对象
		Job job = Job.getInstance(conf, "hotWeahter");
		// 定义执 行类
		job.setJarByClass(HotWeatherApp.class);
		// 设置Mapper和Reducer类
		job.setMapperClass(WeatherMapper.class);
		job.setReducerClass(WeatherReducer.class);
		//添加分区类和Reducer的个数
		job.setPartitionerClass(WeatherPartition.class);
		job.setNumReduceTasks(2);
		// 设置输入和输出路径
		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, out);
		// 设置map和reduce输出的键值对的类型
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		// 等待工作任务完成
		job.waitForCompletion(true);
		System.out.println("done");
	}

	// 定义一个静态内部类，实现Mapper的处理
	public static class WeatherMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, IntWritable, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// 20170601070801+26 20180101120101-10
			String data = value.toString();
			int year = Integer.parseInt(data.substring(0, 4));
			int hot = 0;
			// 截取数据中的第14的符号
			char flag = data.charAt(14);
			if (flag == '+') {
				hot = Integer.parseInt(data.substring(15, 17));
			} else {
				hot = Integer.parseInt(data.substring(14, 17));
			}
			// 将结果写入上下文类中
			context.write(new IntWritable(year), new IntWritable(hot));
		}
	}

	// 定义一个静态内部类，实现Reducer的处理
	// Map输出的键值对， <2017,26>,<2017,22>,<2017,20>,<2017,19>
	// 从map接收过来的数据，reduce会进行重新排序，合并操作，产生新的键值对<2017，list<26,22,20,19>>,然后将该键值对交级reduce进行处理
	public static class WeatherReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
		@Override
		protected void reduce(IntWritable key, Iterable<IntWritable> values,
				Reducer<IntWritable, IntWritable, IntWritable, IntWritable>.Context context)
				throws IOException, InterruptedException {
			int max = Integer.MIN_VALUE;
			for (IntWritable value : values) {
				if (max < value.get()) {
					max = value.get();
				}
			}
			// 将reduce的输出写入上下文类中
			context.write(key, new IntWritable(max));
		}
	}
	
	//按照分区偶数年和奇数年来分区处理
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
