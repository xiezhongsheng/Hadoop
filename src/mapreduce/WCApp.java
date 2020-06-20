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
		// ����Configuration����
		Configuration conf = new Configuration();
		// ͳ�Ƶ��Ǳ���windowsĿ¼�µ��ļ��еĴ�Ƶ��
		// //��������·��
		// Path input = new Path("D:\\bigdata\\wc");
		// //�������·��
		// Path out = new Path("D:\\bigdata\\wc\\out");
		// //�ж����Ŀ¼������ڣ�ֱ��ɾ��
		// FileSystem.get(conf).delete(out,true);
		// ͳ��HDFS���ļ��еĴ�Ƶ
		conf.set("fs.defaultFS", "hdfs://master:9000");
		FileSystem hdfs = FileSystem.get(conf);
		Path input = new Path("/local/words.txt");
		Path out = new Path("/output/count5");
		// ���Ŀ¼���ܴ��ڵģ�������ڣ�����ɾ��
		if (hdfs.exists(out)) {
			hdfs.delete(out, true);
		}
		// ��������JOB
		Job job = Job.getInstance(conf);
		// ������������
		job.setJobName("WCApp");
		// ���ù�����������
		job.setJarByClass(WCApp.class);
		// ���������ʽ��
		job.setInputFormatClass(TextInputFormat.class);
		// �������·��
		FileInputFormat.addInputPath(job, input);
		// ������·��
		FileOutputFormat.setOutputPath(job, out);
		// ����Mapper��
		job.setMapperClass(WCMapper.class);
//		//���úϲ���
//		job.setCombinerClass(WCCombiner.class);
		// ����Reducer��
		job.setReducerClass(WCReducer.class);
		// ����Redcucer�ĸ���
		job.setNumReduceTasks(1);
		
		// ����Map������ļ���ֵ������
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		// ����reduce����ļ���ֵ������
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		/*
		 * ע�⣺���Map��Reduce������ļ���ֵ��������һ���ģ�����ֻ���� setOutputKeyClass��setOutputValueClass���ɡ�
		 * ���map��reduce����ļ�ֵ�Ե����Ͳ�ͬʱ����Ҫ�ֿ��������á�
		 */
		// �ȴ������������
		job.waitForCompletion(true);
		System.out.println("OK");
	}
}