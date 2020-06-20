package hdfstest;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class AddFile {
	public static void main(String[] args) throws IOException {
		//创建Configuration对象
		Configuration conf = new Configuration();
		//设置配置参数信息
		conf.set("fs.defaultFS", "hdfs://master:9000");
		//产生hadoop的文件系统的实例，类是抽象类，不能直接实例化，通过静态方法get()获取文件系统对象
		FileSystem hdfs = FileSystem.get(conf);
		//设置源文件路径
		Path src = new Path("D:\\xzs.txt");
		//设置目标路径
		Path dst = new Path("/data0515");
		//调用文件系统的copyFromLocalFile()方法复制本地文件到hdfs上
		hdfs.copyFromLocalFile(src, dst);
		//查看是否上传成功，调用文件系统listStatus()返回目标目录的文件列表
		FileStatus[] files = hdfs.listStatus(dst);
		//输出目标目录文件列表，打印到控制台
		for(FileStatus file:files) {
			System.out.println(file.getPath());
		}
		
	}
}
