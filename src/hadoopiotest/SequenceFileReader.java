package hadoopiotest;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.zookeeper.common.IOUtils;

public class SequenceFileReader {
	public static void main(String[] args) throws IOException {
		// 1、创建Configuration对象
		Configuration conf = new Configuration();
		// 2、明确设置配置参数信息
		conf.set("fs.defaultFS", "hdfs://master:9000");
		// 3、要读取的文件的路径
		Path path = new Path("/date0522/words.txt");
		// 4、定义Reader对象
		SequenceFile.Reader reader = new SequenceFile.Reader(conf, Reader.file(path));
		Text key = new Text();
		Text value = new Text();
		// 循环读取文件中的记录
		while (reader.next(key, value)) {
			System.out.println(key);
			System.out.println(value);
		}
		// 5、关闭流对象
		IOUtils.closeStream(reader);
	}
}