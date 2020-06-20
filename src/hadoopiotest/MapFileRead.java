package hadoopiotest;

import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.MapFile.Reader;
import org.apache.hadoop.io.Text;

public class MapFileRead {
	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://master:9000");
		
		//MapFileĿ��·��
		Path mapFile = new Path("/data0527/map1");
		
		//Reader����
		MapFile.Reader reader = new Reader(mapFile, conf);
		
		//����KEY��VALUE
		IntWritable key = new IntWritable();
		Text value = new Text();
		
		//ѭ����ȡMapFile�ļ��еļ�¼
		while (reader.next(key, value)) {
			System.out.println(key+ "\t" + value);
		}
		IOUtils.closeQuietly(reader);
	}
}
