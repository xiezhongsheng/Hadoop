package javaiotest;

import java.io.Serializable;

public class Student implements Serializable{
	private int stuId;
	
	private String stuName;
	
	private int age;

	public Student() {
		super();
		// TODO Auto-generated constructor stub
	}

	public Student(int stuId, String stuName, int age) {
		super();
		this.stuId = stuId;
		this.stuName = stuName;
		this.age = age;
	}

	public int getStuId() {
		return stuId;
	}

	public void setStuId(int stuId) {
		this.stuId = stuId;
	}

	public String getStuName() {
		return stuName;
	}

	public void setStuName(String stuName) {
		this.stuName = stuName;
	}

	public int getAge() {
		return age;
	}

	public void setAge(int age) {
		this.age = age;
	}

	@Override
	public String toString() {
		return "Student [stuId=" + stuId + ", stuName=" + stuName + ", age=" + age + "]";
	}
	
}
