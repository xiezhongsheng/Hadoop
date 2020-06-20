package hadoopiotest;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.ObjectWritable;

public class OtherWritable {
	public static void main(String[] args) {
		//通过NullWritable.get()获取NullWritable实例
		NullWritable n = NullWritable.get();
		
		//可以封装多种类型的数据，适用于一个字段需要使用多种类型
		ObjectWritable o = new ObjectWritable("retretreter");
		o.set(null);
		o.set(123);
	}
}
