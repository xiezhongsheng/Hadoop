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
		
		//Writer����
		SequenceFile.Writer writer = SequenceFile.createWriter(conf, Writer.file(dest), Writer.keyClass(Text.class),
				Writer.valueClass(Text.class), Writer.compression(CompressionType.NONE));
		
		//������ָ��Ŀ¼�µ�С�ļ��б�׷�ӵ�SquenceFile�С�
		File[] listFiles = new File("D:\\test").listFiles();
		Text key = null;
		Text value = null;
		
		//�������ص�С�ļ��б����ļ�·����ΪKEY���ļ�������ΪVALUE׷�ӵ��ļ��С�
		for (File file : listFiles) {
			key = new Text(file.getPath());
			value = new Text(FileUtils.readFileToString(file));
			writer.append(key, value);
		}
		
		//�ر�writer��
		IOUtils.closeStream(writer);
		
		//���ļ�������
		SequenceFile.Reader reader = new SequenceFile.Reader(conf, Reader.file(dest));
		Text outKey = new Text();
		Text outValue = new Text();
		
		//����SequenceFile�ļ�
		while (reader.next(outKey, outValue)) {
			System.out.println(outKey.toString());
			System.out.println(outValue.toString());
		}
		
		//�ر�reader��
		IOUtils.closeStream(reader);
	}
}