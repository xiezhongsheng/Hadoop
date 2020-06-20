package javaiotest;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;

public class Student2 implements Externalizable{
	private int stuId;
	
	private String stuName;
	
	private int age;

	public Student2() {
		super();
		// TODO Auto-generated constructor stub
	}

	public Student2(int stuId, String stuName, int age) {
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

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		// 定义序列化的规则
		out.writeInt(this.getStuId());
		out.writeUTF(this.getStuName());
		out.writeInt(this.getAge());
		
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		// 定义反序列化的规则
		this.setStuId(in.readInt());
		this.setStuName(in.readUTF());
		this.setAge(in.readInt());
	}
	
}
