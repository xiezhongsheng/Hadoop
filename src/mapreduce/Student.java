package mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

//����mapreduce���д���ʱ����Student��Ķ�����Ϊkey�����д���
public class Student implements WritableComparable<Student> {
	private String name;
	private int age;

	@Override
	public String toString() {
		return "Student [name=" + name + ", age=" + age + "]";
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getAge() {
		return age;
	}

	public void setAge(int age) {
		this.age = age;
	}

	public Student(String name, int age) {
		super();
		this.name = name;
		this.age = age;
	}

	public Student() {
		super();
		// TODO Auto-generated constructor stub
	}

	@Override
	public void write(DataOutput out) throws IOException {
		//���л�����
		out.writeUTF(this.name);
		out.writeInt(this.age);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		//�����л�����
		this.setName(in.readUTF());
		this.setAge(in.readInt());
	}

	@Override
	public int compareTo(Student o) {
		//�ж����ѧ��������ͬ���Ͱ�������������
		if (this.name.compareTo(o.getName()) == 0) {
			return this.getAge() - o.getAge();
		} else {
			//���ѧ��������ͬ����������������
			return this.name.compareTo(o.getName());
		}
	}
}