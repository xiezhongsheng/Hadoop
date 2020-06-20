package hadoopiotest;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.zookeeper.common.IOUtils;

public class SequenceFileTest {
	public static void main(String[] args) throws IOException {
		// 1、创建Configuration对象
		Configuration conf = new Configuration();
		// 2、明确设置配置参数信息
		conf.set("fs.defaultFS", "hdfs://master:9000");
		// 3、创建文件输出路径
		Path path = new Path("/date0522/words.txt");
		// 4、SequenceFile中内部类Writer用于小文件的写操作，假设Key和value类型都是Text类型。
		SequenceFile.Writer writer = SequenceFile.createWriter(conf,
				// 需要指定文件的输出路径
				Writer.file(path),
				// 指定KEY的类型
				Writer.keyClass(Text.class),
				// 指定VALUE的类型
				Writer.valueClass(Text.class),
				// 指定压缩类型
				/*
				 * 1、不压缩：CompressionType.NONE 2、记录级别的压缩 ：CompressionType.RECORD
				 * 3、块级别的压缩：CompressionType.BLOCK
				 */
				Writer.compression(CompressionType.NONE));
		// 5、通过writer向文档中写入记录
		writer.append(new Text("hello"), new Text("world"));
		// 6、关闭writer流
		IOUtils.closeStream(writer);
		System.out.println("写完了");
	}
}
