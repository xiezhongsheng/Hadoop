package hadoopiotest;

import org.apache.hadoop.io.IntWritable;

public class IntWritableTest {
	public static void main(String[] args) {
		IntWritable i = new IntWritable(123);//��������ʱ��ֵ
		System.out.println(i.get());//ͨ��get()������ȡIntWritable�����ֵ
		i.set(345);//ͨ��set()������IntWritable����ֵ
		System.out.println(i.get());
	}
}