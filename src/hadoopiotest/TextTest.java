package hadoopiotest;

import java.io.UnsupportedEncodingException;
import org.apache.hadoop.io.Text;

public class TextTest {
	public static void main(String[] args) throws UnsupportedEncodingException {
		// 定义Text对象
		Text t = new Text("你好呀");
		// 定义String对象
		String s = "你好呀";
		// 测试Text对象的字节数之和是多少 UTF-8中中文字符一个字符占3个字节
		System.out.println(t.getLength()); // 9
		System.out.println(s.getBytes("utf-8").length); // 9
		// String对象的长度对应char编码单元的个数
		System.out.println(s.length()); // 3
		// Text类中的find()方法返回的是当前位置的字节偏移量
		System.out.println(t.find("呀")); // 6
		// String类的indexOf()方法返回的是char编码单元中的索引位置
		System.out.println(s.indexOf("呀")); // 2
	}
}
