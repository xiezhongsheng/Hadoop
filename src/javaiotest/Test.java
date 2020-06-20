package javaiotest;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import javax.imageio.spi.IIOServiceProvider;


public class Test {
	public static void main(String[] args) {
		try {
			readObject();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	//序列化过程，将学生对象永久存入本地文件中
	public static void writeObjrct() throws FileNotFoundException, IOException {
		//定义文件对象
		File f = new File("d:\\stu.dat");
		//定义要写入的对象
		Student2 stu = new Student2(111,"lisi",21);
		//定义文件输出流
		ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(f));
		//将对象写入本地文件中
		oos.writeObject(stu);
		System.out.println("done");
		
	}
	
	//反序列化过程，将文件中存储的学生对象恢复到程序中
	public static void readObject() throws Exception {
		//定义文件对象
		File f = new File("d:\\stu.dat");
		//定义输入流
		ObjectInputStream ois = new ObjectInputStream(new FileInputStream(f));
		//读对象到程序中
		Student2 s =(Student2)ois.readObject();
		System.out.println(s);
	}
}
