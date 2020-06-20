package mapreduce;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StudentApp {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://master:9000");
		Path input = new Path("/local/stu.txt");
		Path out = new Path("/output/stuCount");
		//����������������
		Job job = Job.getInstance(conf, "studnet");
		job.setJarByClass(StudentApp.class);
		job.setMapperClass(StuMapper.class);
		job.setReducerClass(StuReducer.class);
		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, out);
		job.setMapOutputKeyClass(Student.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.waitForCompletion(true);
		System.out.println("OK");
	}

	//1.Mapper����
	public static class StuMapper extends Mapper<LongWritable, Text, Student, IntWritable> {
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Student, IntWritable>.Context context)
				throws IOException, InterruptedException {
			//�����ļ���ÿ��������ѧ�������������� ���Կո�ָ�
			String[] vals = value.toString().split(" ");
			//��ÿ�����ݷ�װΪѧ������
			Student stu = new Student(vals[0], Integer.parseInt(vals[1]));
			context.write(stu, new IntWritable(1));
		}
	}

	//2.Reducer����
	public static class StuReducer extends Reducer<Student, IntWritable, Text, IntWritable> {
		@Override
		protected void reduce(Student key, Iterable<IntWritable> value,
				Reducer<Student, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			Text outKey = new Text(key.getName());
			IntWritable outValue = new IntWritable(key.getAge());
			context.write(outKey, outValue);
		}
	}
}