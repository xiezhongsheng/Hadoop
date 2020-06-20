package hadoopiotest;

import static org.junit.Assert.fail;

import java.io.IOException;

import javax.swing.RowFilter.ComparisonType;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.MapFile.Writer;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.yarn.webapp.example.HelloWorld.Hello;


public class MapFileTest {
	//定义字符串数组
	private static final String[] myValue = {"hello world", "bye world", "hello hadoop", "bye hadoop"};
	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://master:9000");
		
		//MapFile目标路径
		Path mapFile = new Path("/data0527/map1");
		
		//Writer对象
		MapFile.Writer writer = new MapFile.Writer(conf, mapFile, 
				Writer.keyClass(IntWritable.class), 
				Writer.valueClass(Text.class),
				Writer.compression(CompressionType.NONE));
		
		//定义KEY、VALUE
		IntWritable key = new IntWritable();
		Text value = new Text();
		
		//循环写入
		for (int i = 0; i < 500; i++) {
			key.set(i);
			value.set(myValue[i % myValue.length]);
			//追加内容到MapFile
			writer.append(key, value);
		}
		
		//关闭流
		IOUtils.closeQuietly(writer);
		
	}

}
