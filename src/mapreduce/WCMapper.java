package mapreduce;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
* 继承Mapper类，重写map方法
*/
public class WCMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	@Override
	//重写父类的map方法
	//<字符的偏移量,一行文本> ，map要做的事情，就是将这一行文本切割成一个个单词，输出<单词,1>
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		//定义输出key的类型
		Text keyOut = new Text();
		//定义输出value的类型
		IntWritable valueOut = new IntWritable(1);
		//切割输入的值，切割成一个个的单词
		String[] arr = value.toString().split(" ");
		for (String s : arr) {
			keyOut.set(s);
			//写入MAP处理的结果到上下文类中
			context.write(keyOut, valueOut);
		}
	}
}