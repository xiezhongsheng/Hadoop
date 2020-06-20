package hdfstest;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class CreateFile {
	public static void main(String[] args) throws IOException {
		//创建Configuration对象
		Configuration conf = new Configuration();
		//设置配置参数信息
		conf.set("fs.defaultFS", "hdfs://master:9000");
		//产生hdfs文件系统对象
		FileSystem hdfs = FileSystem.get(conf);
		//定义写入的内容
		byte[] buff = "hello world".getBytes();
		//定义目标路径
		Path dst = new Path("/data0515/tmp.txt");
		//通过文件系统create()得到输出流FSDataOutputStream
		FSDataOutputStream os = hdfs.create(dst);
		//利用输出流往文件中写入数据
		os.write(buff, 0, buff.length);
		os.close();
		boolean res = hdfs.exists(dst);
		
		System.out.println(res?"文件创建成功！":"文件创建失败！");
	}
}
