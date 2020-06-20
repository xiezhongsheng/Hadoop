package sort;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Test {
	public static void main(String[] args) {
		List<Student> stus = new ArrayList();
		stus.add(new Student("zhangsan",24));
		stus.add(new Student("chenxiang",21));
		stus.add(new Student("lisi",22));
		
		System.out.println("-----------ԭʼ����------------");
		for(Student stu : stus) {
			System.out.println(stu);
		}
		
		System.out.println("-----------Ĭ������------------");
		Collections.sort(stus);
		for(Student stu : stus) {
			System.out.println(stu);
		}
		
		System.out.println("-----------����ѧ��������������------------");
		stus.sort(new StudentSortByNameDesc());
		for(Student stu : stus) {
			System.out.println(stu);
		}

		System.out.println("--------------��������������------------");
		stus.sort(new StudentSortByAgeAsc());
		for(Student stu:stus) {
		System.out.println(stu);
		}
	}
}
