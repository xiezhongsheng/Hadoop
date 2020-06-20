package hdfstest;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ReadFile {
	public static void main(String[] args) throws IOException {
		//创建Configuration对象
		Configuration conf = new Configuration();
		//设置配置参数信息
		conf.set("fs.defaultFS", "hdfs://master:9000");
		//产生hdfs文件系统对象
		FileSystem hdfs = FileSystem.get(conf);
		//定义要读取的文件的路径
		Path dst = new Path("/data0515/xzs.txt");
		//通过文件系统的open()方法来获得输入流对象
		FSDataInputStream fis = hdfs.open(dst);
		int i = fis.read();
		while(i != -1) {
			System.out.print((char)i);
			i = fis.read();
		}
		fis.close();
	}
}
