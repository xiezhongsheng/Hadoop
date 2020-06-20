package mapreduce;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import sort.Student;

public class StudentAppByNameGroup {
	//自定义Mapper
	public static class StuMapper extends Mapper<LongWritable, Text, Student, IntWritable>{
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Student, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// 数据文件的每行内容是学生的姓名和年龄，以空格分隔
			String[] vals = value.toString().split(" ");
			// 将每行数据封装为学生对象
			Student stu = new Student(vals[0], Integer.parseInt(vals[1]));
			context.write(stu, new IntWritable(stu.getAge()));
		}
	}
	//自定义分组类
	public static class MyGroup implements RawComparator<Student>{
		@Override
		public int compare(Student s1, Student s2) {
		//分组策略中，这个方法可以不写
		return 0;
		}
		@Override
		/**
		* b1 表示第一个参与比较的字节数组
		* s1 表示第一个字节数组中开始比较的位置
		* l1 表示第一个字节数组中参与比较的字节长度
		* b2 表示第二个参与比较的字节数组
		* s2 表示第二个字节数组中开始比较的位置
		* l2 表示第二个字节数组参与比较的字节长度
		*/
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
		{
			/* 这里是按第Student中的第一个元素进行分组，因为是Student中第二个元素是int类型，长度是4个字节
			* 因此参与比较的字节长度减去4，就是要比较的第一个元素的字节长度
			*/
			return WritableComparator.compareBytes(b1, s1, l1-4, b2, s2, l2-4);
		}
	}
	//自定义Redecer类
	public static class StuReducer extends Reducer<Student, IntWritable, Text, IntWritable>{

		@Override
		protected void reduce(Student key, Iterable<IntWritable> values,
				Reducer<Student, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			int min = Integer.MAX_VALUE;
			for(IntWritable i :values) {
				if(min > i.get()) {
					min = i.get();
				}
			}
			context.write(new Text(key.getName()), new IntWritable(min));
		}
		
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://master:9000");
		FileSystem hdfs = FileSystem.get(conf);
		Path input = new Path("/local/stu.txt");
		Path out = new Path("/output/scount");
		if (hdfs.exists(out)) {
			hdfs.delete(out, true);
		}
		Job job = Job.getInstance(conf, "student");
		job.setJarByClass(StudentAppByNameGroup.class);
		job.setMapperClass(StuMapper.class);
		job.setReducerClass(StuReducer.class);
		// 定义自定义分组类
		job.setGroupingComparatorClass(MyGroup.class);
		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, out);
		job.setMapOutputKeyClass(Student.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.waitForCompletion(true);
		System.out.println("OK");
	}
}