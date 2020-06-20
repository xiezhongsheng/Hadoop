package hdfstest;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class CreateFile {
	public static void main(String[] args) throws IOException {
		//����Configuration����
		Configuration conf = new Configuration();
		//�������ò�����Ϣ
		conf.set("fs.defaultFS", "hdfs://master:9000");
		//����hdfs�ļ�ϵͳ����
		FileSystem hdfs = FileSystem.get(conf);
		//����д�������
		byte[] buff = "hello world".getBytes();
		//����Ŀ��·��
		Path dst = new Path("/data0515/tmp.txt");
		//ͨ���ļ�ϵͳcreate()�õ������FSDataOutputStream
		FSDataOutputStream os = hdfs.create(dst);
		//������������ļ���д������
		os.write(buff, 0, buff.length);
		os.close();
		boolean res = hdfs.exists(dst);
		
		System.out.println(res?"�ļ������ɹ���":"�ļ�����ʧ�ܣ�");
	}
}
