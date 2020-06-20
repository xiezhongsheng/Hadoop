package hadoopiotest;

import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;

public class SequenceFileToMapFile {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://master:9000");
		
		//�����ļ�ϵͳ
		FileSystem hdfs = FileSystem.get(conf);
		
		//����MapFile���õ�Ŀ��·��
		Path map = new Path("/data0527/map2");
		
		//SequenceFile �ļ�·��
		Path seq = new Path("/data0522/local.txt");
		
		//ͨ��SequenceFile.Reader��ȡ����
		SequenceFile.Reader reader = new SequenceFile.Reader(conf, Reader.file(seq));
		
		Class key = reader.getKeyClass();
		Class value = reader.getClass();
		
		reader.close();
		
		//����MapFile.fix()��SequenceFile�ļ�תΪMapFile
		//ǰ�᣺Ҫ��SequenceFile��ֵ��MapFileĿ��·���£�����������Ϊdata
		
		long res = MapFile.fix(hdfs, map, key, value, false, conf);
		System.out.println(res);
	}
}
