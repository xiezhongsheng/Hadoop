package hadoopiotest;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.ObjectWritable;

public class OtherWritable {
	public static void main(String[] args) {
		//ͨ��NullWritable.get()��ȡNullWritableʵ��
		NullWritable n = NullWritable.get();
		
		//���Է�װ�������͵����ݣ�������һ���ֶ���Ҫʹ�ö�������
		ObjectWritable o = new ObjectWritable("retretreter");
		o.set(null);
		o.set(123);
	}
}
