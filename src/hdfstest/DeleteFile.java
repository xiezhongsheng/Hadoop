package hdfstest;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class DeleteFile {
	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://master:9000");
		
		FileSystem hdfs = FileSystem.get(conf);
		Path dst = new Path("/data0515/xzs.txt");
		
		boolean ret = hdfs.delete(dst, false);
		//第二个参数: 目标为目录的话true递归删除，目标为文件true/false无所谓
		System.out.println(ret ? "文件删除成功！":"文件删除失败！");
	}
}
