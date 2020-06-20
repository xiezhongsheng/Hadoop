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
	//�Զ���Mapper
	public static class StuMapper extends Mapper<LongWritable, Text, Student, IntWritable>{
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Student, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// �����ļ���ÿ��������ѧ�������������䣬�Կո�ָ�
			String[] vals = value.toString().split(" ");
			// ��ÿ�����ݷ�װΪѧ������
			Student stu = new Student(vals[0], Integer.parseInt(vals[1]));
			context.write(stu, new IntWritable(stu.getAge()));
		}
	}
	//�Զ��������
	public static class MyGroup implements RawComparator<Student>{
		@Override
		public int compare(Student s1, Student s2) {
		//��������У�����������Բ�д
		return 0;
		}
		@Override
		/**
		* b1 ��ʾ��һ������Ƚϵ��ֽ�����
		* s1 ��ʾ��һ���ֽ������п�ʼ�Ƚϵ�λ��
		* l1 ��ʾ��һ���ֽ������в���Ƚϵ��ֽڳ���
		* b2 ��ʾ�ڶ�������Ƚϵ��ֽ�����
		* s2 ��ʾ�ڶ����ֽ������п�ʼ�Ƚϵ�λ��
		* l2 ��ʾ�ڶ����ֽ��������Ƚϵ��ֽڳ���
		*/
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
		{
			/* �����ǰ���Student�еĵ�һ��Ԫ�ؽ��з��飬��Ϊ��Student�еڶ���Ԫ����int���ͣ�������4���ֽ�
			* ��˲���Ƚϵ��ֽڳ��ȼ�ȥ4������Ҫ�Ƚϵĵ�һ��Ԫ�ص��ֽڳ���
			*/
			return WritableComparator.compareBytes(b1, s1, l1-4, b2, s2, l2-4);
		}
	}
	//�Զ���Redecer��
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
		// �����Զ��������
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