package hdfstest;

import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;

/*
* ͨ��javaʵ����ʾ�ļ����ݣ����䣩
*/
public class ReadFileJava {
	public static void main(String[] args) throws Exception {
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
		//����URLʵ��
		URL url = new URL("hdfs://master:9000/date0515/tmp.txt");
		URLConnection conn = url.openConnection();
		InputStream is = conn.getInputStream();
		byte[] buf = new byte[32];
		while (true) {
			int len = is.read(buf, 0, buf.length);
			if (len == -1) {
				break;
			}
			System.out.print(new String(buf).substring(0, len));
		}
	}
}