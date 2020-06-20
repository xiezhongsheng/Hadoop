package hadoopiotest;

import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.zookeeper.common.IOUtils;

public class SequenceFileWRLocalFile {
	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://master:9000");
		Path dest = new Path("/data0522/local.txt");
		
		//Writer对象
		SequenceFile.Writer writer = SequenceFile.createWriter(conf, Writer.file(dest), Writer.keyClass(Text.class),
				Writer.valueClass(Text.class), Writer.compression(CompressionType.NONE));
		
		//将本地指定目录下的小文件列表追加到SquenceFile中。
		File[] listFiles = new File("D:\\test").listFiles();
		Text key = null;
		Text value = null;
		
		//遍历本地的小文件列表，将文件路径做为KEY，文件内容做为VALUE追加到文件中。
		for (File file : listFiles) {
			key = new Text(file.getPath());
			value = new Text(FileUtils.readFileToString(file));
			writer.append(key, value);
		}
		
		//关闭writer流
		IOUtils.closeStream(writer);
		
		//读文件的内容
		SequenceFile.Reader reader = new SequenceFile.Reader(conf, Reader.file(dest));
		Text outKey = new Text();
		Text outValue = new Text();
		
		//遍历SequenceFile文件
		while (reader.next(outKey, outValue)) {
			System.out.println(outKey.toString());
			System.out.println(outValue.toString());
		}
		
		//关闭reader流
		IOUtils.closeStream(reader);
	}
}