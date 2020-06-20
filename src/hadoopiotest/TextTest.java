package hadoopiotest;

import java.io.UnsupportedEncodingException;
import org.apache.hadoop.io.Text;

public class TextTest {
	public static void main(String[] args) throws UnsupportedEncodingException {
		// ����Text����
		Text t = new Text("���ѽ");
		// ����String����
		String s = "���ѽ";
		// ����Text������ֽ���֮���Ƕ��� UTF-8�������ַ�һ���ַ�ռ3���ֽ�
		System.out.println(t.getLength()); // 9
		System.out.println(s.getBytes("utf-8").length); // 9
		// String����ĳ��ȶ�Ӧchar���뵥Ԫ�ĸ���
		System.out.println(s.length()); // 3
		// Text���е�find()�������ص��ǵ�ǰλ�õ��ֽ�ƫ����
		System.out.println(t.find("ѽ")); // 6
		// String���indexOf()�������ص���char���뵥Ԫ�е�����λ��
		System.out.println(s.indexOf("ѽ")); // 2
	}
}
