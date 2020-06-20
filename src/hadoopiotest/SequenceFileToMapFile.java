package hadoopiotest;

import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;

public class SequenceFileToMapFile {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://master:9000");
		
		//创建文件系统
		FileSystem hdfs = FileSystem.get(conf);
		
		//定义MapFile放置的目标路径
		Path map = new Path("/data0527/map2");
		
		//SequenceFile 文件路径
		Path seq = new Path("/data0522/local.txt");
		
		//通过SequenceFile.Reader读取内容
		SequenceFile.Reader reader = new SequenceFile.Reader(conf, Reader.file(seq));
		
		Class key = reader.getKeyClass();
		Class value = reader.getClass();
		
		reader.close();
		
		//调用MapFile.fix()将SequenceFile文件转为MapFile
		//前提：要将SequenceFile赋值到MapFile目标路径下，并且重命名为data
		
		long res = MapFile.fix(hdfs, map, key, value, false, conf);
		System.out.println(res);
	}
}
