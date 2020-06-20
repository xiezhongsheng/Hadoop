package hdfstest;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class DeleteFile {
	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://master:9000");
		
		FileSystem hdfs = FileSystem.get(conf);
		Path dst = new Path("/data0515/xzs.txt");
		
		boolean ret = hdfs.delete(dst, false);
		//�ڶ�������: Ŀ��ΪĿ¼�Ļ�true�ݹ�ɾ����Ŀ��Ϊ�ļ�true/false����ν
		System.out.println(ret ? "�ļ�ɾ���ɹ���":"�ļ�ɾ��ʧ�ܣ�");
	}
}
