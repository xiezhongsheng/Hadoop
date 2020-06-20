package hdfstest;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class AddFile {
	public static void main(String[] args) throws IOException {
		//����Configuration����
		Configuration conf = new Configuration();
		//�������ò�����Ϣ
		conf.set("fs.defaultFS", "hdfs://master:9000");
		//����hadoop���ļ�ϵͳ��ʵ�������ǳ����࣬����ֱ��ʵ������ͨ����̬����get()��ȡ�ļ�ϵͳ����
		FileSystem hdfs = FileSystem.get(conf);
		//����Դ�ļ�·��
		Path src = new Path("D:\\xzs.txt");
		//����Ŀ��·��
		Path dst = new Path("/data0515");
		//�����ļ�ϵͳ��copyFromLocalFile()�������Ʊ����ļ���hdfs��
		hdfs.copyFromLocalFile(src, dst);
		//�鿴�Ƿ��ϴ��ɹ��������ļ�ϵͳlistStatus()����Ŀ��Ŀ¼���ļ��б�
		FileStatus[] files = hdfs.listStatus(dst);
		//���Ŀ��Ŀ¼�ļ��б���ӡ������̨
		for(FileStatus file:files) {
			System.out.println(file.getPath());
		}
		
	}
}
