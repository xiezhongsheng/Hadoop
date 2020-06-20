package hdfstest;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FileList {
	public static void main(String[] args) throws IOException {
		//创建Configuration对象
		Configuration conf = new Configuration();
		//设置配置参数信息
		conf.set("fs.defaultFS", "hdfs://master:9000");
		//产生hdfs文件系统对象
		FileSystem hdfs = FileSystem.get(conf);
		//定义目标目录
		Path dst = new Path("/input");
		FileStatus[] files = hdfs.listStatus(dst);
		//输出目标目录文件列表，打印到控制台
		for(FileStatus file:files) {
			System.out.println(file);
		}
	}
}
