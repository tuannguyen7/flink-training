package org.apache.flink.training.experiment.pojo;

public class Student {

  public String name;
  public double mark;

  public Student() {}
  public Student(String name, double mark) {
    this.name = name;
    this.mark = mark;
  }

  @Override
  public String toString() {
    return "Student{" +
        "name='" + name + '\'' +
        ", mark=" + mark +
        '}';
  }
}
