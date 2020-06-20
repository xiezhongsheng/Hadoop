package hdfstest;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FileList {
	public static void main(String[] args) throws IOException {
		//����Configuration����
		Configuration conf = new Configuration();
		//�������ò�����Ϣ
		conf.set("fs.defaultFS", "hdfs://master:9000");
		//����hdfs�ļ�ϵͳ����
		FileSystem hdfs = FileSystem.get(conf);
		//����Ŀ��Ŀ¼
		Path dst = new Path("/input");
		FileStatus[] files = hdfs.listStatus(dst);
		//���Ŀ��Ŀ¼�ļ��б���ӡ������̨
		for(FileStatus file:files) {
			System.out.println(file);
		}
	}
}
