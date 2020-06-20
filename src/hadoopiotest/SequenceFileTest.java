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
		// 1������Configuration����
		Configuration conf = new Configuration();
		// 2����ȷ�������ò�����Ϣ
		conf.set("fs.defaultFS", "hdfs://master:9000");
		// 3�������ļ����·��
		Path path = new Path("/date0522/words.txt");
		// 4��SequenceFile���ڲ���Writer����С�ļ���д����������Key��value���Ͷ���Text���͡�
		SequenceFile.Writer writer = SequenceFile.createWriter(conf,
				// ��Ҫָ���ļ������·��
				Writer.file(path),
				// ָ��KEY������
				Writer.keyClass(Text.class),
				// ָ��VALUE������
				Writer.valueClass(Text.class),
				// ָ��ѹ������
				/*
				 * 1����ѹ����CompressionType.NONE 2����¼�����ѹ�� ��CompressionType.RECORD
				 * 3���鼶���ѹ����CompressionType.BLOCK
				 */
				Writer.compression(CompressionType.NONE));
		// 5��ͨ��writer���ĵ���д���¼
		writer.append(new Text("hello"), new Text("world"));
		// 6���ر�writer��
		IOUtils.closeStream(writer);
		System.out.println("д����");
	}
}
