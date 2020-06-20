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
		
		//��HDFS�ϴ�����ʹ��ѹ����SequenceFile
		SequenceFile.Writer writer = SequenceFile.createWriter(conf, Writer.file(targetPath),
				Writer.keyClass(Text.class), Writer.valueClass(Text.class), Writer.compression(CompressionType.NONE));
		
		//�����ļ�ϵͳ��listStatus()����Ŀ��Ŀ¼���ļ��б�
		FileStatus files[] = fs.listStatus(dst);
		Text key = null;
		Text value = null;
		
		//�������ص�С�ļ��б����ļ�·����ΪKEY���ļ�������Ϊvalue׷�ӵ��ĵ���
		for (FileStatus file : files) {
			key = new Text(file.getPath().toString());
			//��ȡ�ļ�������Ϊvalue
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
		//�ر�writer��
		IOUtils.closeStream(writer);
		
		//��ȡHDFS��ָ��Ŀ¼�µ�SequenceFile�ļ�
		SequenceFile.Reader reader = new SequenceFile.Reader(conf, Reader.file(targetPath));
		Text outputKey = new Text();
		Text outputValue = new Text();
		
		//����SequenceFile�ļ���ÿ�м�¼���������̨
		while (reader.next(outputKey, outputValue)) {
			System.out.println(outputKey.toString());
			System.out.println(outputValue.toString());
		}
		
		//�ر�reader��
		IOUtils.closeStream(reader);
	}
}