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
	//�����ַ�������
	private static final String[] myValue = {"hello world", "bye world", "hello hadoop", "bye hadoop"};
	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://master:9000");
		
		//MapFileĿ��·��
		Path mapFile = new Path("/data0527/map1");
		
		//Writer����
		MapFile.Writer writer = new MapFile.Writer(conf, mapFile, 
				Writer.keyClass(IntWritable.class), 
				Writer.valueClass(Text.class),
				Writer.compression(CompressionType.NONE));
		
		//����KEY��VALUE
		IntWritable key = new IntWritable();
		Text value = new Text();
		
		//ѭ��д��
		for (int i = 0; i < 500; i++) {
			key.set(i);
			value.set(myValue[i % myValue.length]);
			//׷�����ݵ�MapFile
			writer.append(key, value);
		}
		
		//�ر���
		IOUtils.closeQuietly(writer);
		
	}

}
