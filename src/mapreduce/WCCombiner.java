package mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WCCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
	@Override
	//该函数的处理与Reducer是相同的，区别就在于该类是用于map处理后的合并。
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		int count = 0;
		for (IntWritable value : values) {
			count = count + value.get();
		}
		context.write(key, new IntWritable(count));
	}
}
