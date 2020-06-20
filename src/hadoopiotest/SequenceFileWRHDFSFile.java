package hadoopiotest;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.zookeeper.common.IOUtils;

public class SequenceFileWRHDFSFile {
	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.198.131:9000");
		
		FileSystem fs = FileSystem.get(conf);
		Path targetPath = new Path("/date0522/hdfs1.txt");
		Path dst = new Path("/local");
		
		//在HDFS上创建不使用压缩的SequenceFile
		SequenceFile.Writer writer = SequenceFile.createWriter(conf, Writer.file(targetPath),
				Writer.keyClass(Text.class), Writer.valueClass(Text.class), Writer.compression(CompressionType.NONE));
		
		//调用文件系统的listStatus()返回目标目录的文件列表
		FileStatus files[] = fs.listStatus(dst);
		Text key = null;
		Text value = null;
		
		//遍历本地的小文件列表，将文件路径做为KEY，文件内容做为value追加到文档中
		for (FileStatus file : files) {
			key = new Text(file.getPath().toString());
			//读取文件内容做为value
			FSDataInputStream fis = fs.open(file.getPath());
			StringBuffer sb = new StringBuffer();
			int i = fis.read();
			while (i != -1) {
				sb.append((char) i);
				i = fis.read();
			}
			fis.close();
			value = new Text(sb.toString());
			writer.append(key, value);
		}
		//关闭writer流
		IOUtils.closeStream(writer);
		
		//读取HDFS上指定目录下的SequenceFile文件
		SequenceFile.Reader reader = new SequenceFile.Reader(conf, Reader.file(targetPath));
		Text outputKey = new Text();
		Text outputValue = new Text();
		
		//遍历SequenceFile文件的每行记录输出到控制台
		while (reader.next(outputKey, outputValue)) {
			System.out.println(outputKey.toString());
			System.out.println(outputValue.toString());
		}
		
		//关闭reader流
		IOUtils.closeStream(reader);
	}
}