package mapreduce;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
* �̳�Mapper�࣬��дmap����
*/
public class WCMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	@Override
	//��д�����map����
	//<�ַ���ƫ����,һ���ı�> ��mapҪ�������飬���ǽ���һ���ı��и��һ�������ʣ����<����,1>
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		//�������key������
		Text keyOut = new Text();
		//�������value������
		IntWritable valueOut = new IntWritable(1);
		//�и������ֵ���и��һ�����ĵ���
		String[] arr = value.toString().split(" ");
		for (String s : arr) {
			keyOut.set(s);
			//д��MAP����Ľ��������������
			context.write(keyOut, valueOut);
		}
	}
}