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
	
	//���л����̣���ѧ���������ô��뱾���ļ���
	public static void writeObjrct() throws FileNotFoundException, IOException {
		//�����ļ�����
		File f = new File("d:\\stu.dat");
		//����Ҫд��Ķ���
		Student2 stu = new Student2(111,"lisi",21);
		//�����ļ������
		ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(f));
		//������д�뱾���ļ���
		oos.writeObject(stu);
		System.out.println("done");
		
	}
	
	//�����л����̣����ļ��д洢��ѧ������ָ���������
	public static void readObject() throws Exception {
		//�����ļ�����
		File f = new File("d:\\stu.dat");
		//����������
		ObjectInputStream ois = new ObjectInputStream(new FileInputStream(f));
		//�����󵽳�����
		Student2 s =(Student2)ois.readObject();
		System.out.println(s);
	}
}
