package hadoopiotest;

import org.apache.hadoop.io.IntWritable;

public class IntWritableTest {
	public static void main(String[] args) {
		IntWritable i = new IntWritable(123);//创建对象时赋值
		System.out.println(i.get());//通过get()方法获取IntWritable对象的值
		i.set(345);//通过set()方法给IntWritable对象赋值
		System.out.println(i.get());
	}
}