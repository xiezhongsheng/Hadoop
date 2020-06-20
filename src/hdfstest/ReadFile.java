package hdfstest;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ReadFile {
	public static void main(String[] args) throws IOException {
		//����Configuration����
		Configuration conf = new Configuration();
		//�������ò�����Ϣ
		conf.set("fs.defaultFS", "hdfs://master:9000");
		//����hdfs�ļ�ϵͳ����
		FileSystem hdfs = FileSystem.get(conf);
		//����Ҫ��ȡ���ļ���·��
		Path dst = new Path("/data0515/xzs.txt");
		//ͨ���ļ�ϵͳ��open()�������������������
		FSDataInputStream fis = hdfs.open(dst);
		int i = fis.read();
		while(i != -1) {
			System.out.print((char)i);
			i = fis.read();
		}
		fis.close();
	}
}
