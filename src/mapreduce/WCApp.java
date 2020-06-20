package mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WCApp {
	public static void main(String[] args) throws Exception {
		// 定义Configuration对象
		Configuration conf = new Configuration();
		// 统计的是本地windows目录下的文件中的词频。
		// //定义输入路径
		// Path input = new Path("D:\\bigdata\\wc");
		// //定义输出路径
		// Path out = new Path("D:\\bigdata\\wc\\out");
		// //判断输出目录如果存在，直接删除
		// FileSystem.get(conf).delete(out,true);
		// 统计HDFS上文件中的词频
		conf.set("fs.defaultFS", "hdfs://master:9000");
		FileSystem hdfs = FileSystem.get(conf);
		Path input = new Path("/local/words.txt");
		Path out = new Path("/output/count5");
		// 输出目录不能存在的，如果存在，将其删除
		if (hdfs.exists(out)) {
			hdfs.delete(out, true);
		}
		// 定义任务JOB
		Job job = Job.getInstance(conf);
		// 设置任务名称
		job.setJobName("WCApp");
		// 设置工作任务类名
		job.setJarByClass(WCApp.class);
		// 设置输入格式类
		job.setInputFormatClass(TextInputFormat.class);
		// 添加输入路径
		FileInputFormat.addInputPath(job, input);
		// 添加输出路径
		FileOutputFormat.setOutputPath(job, out);
		// 设置Mapper类
		job.setMapperClass(WCMapper.class);
//		//设置合并类
//		job.setCombinerClass(WCCombiner.class);
		// 设置Reducer类
		job.setReducerClass(WCReducer.class);
		// 设置Redcucer的个数
		job.setNumReduceTasks(1);
		
		// 设置Map端输出的键和值的类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		// 设置reduce输出的键和值的类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		/*
		 * 注意：如果Map和Reduce的输出的键和值的类型是一样的，可以只设置 setOutputKeyClass和setOutputValueClass即可。
		 * 如果map和reduce输出的键值对的类型不同时，需要分开进行设置。
		 */
		// 等待工作任务完成
		job.waitForCompletion(true);
		System.out.println("OK");
	}
}