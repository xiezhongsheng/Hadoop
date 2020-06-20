package mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

//想在mapreduce进行处理时，将Student类的对象做为key来进行处理
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
		//序列化规则
		out.writeUTF(this.name);
		out.writeInt(this.age);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		//反序列化规则
		this.setName(in.readUTF());
		this.setAge(in.readInt());
	}

	@Override
	public int compareTo(Student o) {
		//判断如果学生姓名相同，就按年龄升序排序
		if (this.name.compareTo(o.getName()) == 0) {
			return this.getAge() - o.getAge();
		} else {
			//如果学生姓名不同，按姓名升序排序
			return this.name.compareTo(o.getName());
		}
	}
}