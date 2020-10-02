package org.apache.flink.training.experiment.generators;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.training.experiment.pojo.Student;

public class StudentGenerator implements SourceFunction<Student> {

  @Override
  public void run(SourceContext<Student> ctx) throws Exception {
    Student s1 = new Student("a", 5);
    Student s11 = new Student("a", 3);
    Student s2 = new Student("b", 6);
    Student s21 = new Student("b", 6);
    Student s3 = new Student("c", 5.5);
    Student s4 = new Student("d", 4);
    Student s14 = new Student("a", 7);
    List<Student> students = Stream.of(s1, s11, s2, s21, s3, s4, s14).collect(Collectors.toList());
    for (Student s : students) {
      Long now = System.currentTimeMillis();
      ctx.collectWithTimestamp(s, now);
      ctx.emitWatermark(new Watermark(now));
      Thread.sleep(1000);
    }
  }

  @Override
  public void cancel() {

  }
}
