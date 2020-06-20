package hadoopiotest;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

public class Student implements WritableComparable<Student> {
	private int stuId;
	private String stuName;
	private int age;

	@Override
	public String toString() {
		return "Student [stuId=" + stuId + ", stuName=" + stuName + ", age=" + age + "]";
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

	public Student(int stuId, String stuName, int age) {
		super();
		this.stuId = stuId;
		this.stuName = stuName;
		this.age = age;
	}

	public Student() {
		super();
		// TODO Auto-generated constructor stub
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// hadoop���л�
		out.writeInt(this.getStuId());
		out.writeUTF(this.getStuName());
		out.writeInt(this.getAge());
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// �����л�
		this.setStuId(in.readInt());
		this.setStuName(in.readUTF());
		this.setAge(in.readInt());
	}

	@Override
	public int compareTo(Student o) {
		//�ƶ��Ƚϵ�����
		if (this.stuId < o.getStuId()) {
			return -1;
		} else if (this.stuId > o.getStuId()) {
			return 1;
		}
		return 0;
	}
}
