package hadoopiotest;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.zookeeper.common.IOUtils;

public class SequenceFileReader {
	public static void main(String[] args) throws IOException {
		// 1������Configuration����
		Configuration conf = new Configuration();
		// 2����ȷ�������ò�����Ϣ
		conf.set("fs.defaultFS", "hdfs://master:9000");
		// 3��Ҫ��ȡ���ļ���·��
		Path path = new Path("/date0522/words.txt");
		// 4������Reader����
		SequenceFile.Reader reader = new SequenceFile.Reader(conf, Reader.file(path));
		Text key = new Text();
		Text value = new Text();
		// ѭ����ȡ�ļ��еļ�¼
		while (reader.next(key, value)) {
			System.out.println(key);
			System.out.println(value);
		}
		// 5���ر�������
		IOUtils.closeStream(reader);
	}
}