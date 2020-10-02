package org.apache.flink.training.experiment.generators;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.training.experiment.pojo.Student;

public class SingleStudentGenerator implements SourceFunction<Student> {

  @Override
  public void run(SourceContext<Student> ctx) throws Exception {
    int mark = 0;
    Student s1 = new Student("a", mark++);
    Student s11 = new Student("a", mark++);
    Student s12 = new Student("a", mark++);
    Student s13 = new Student("a", mark++);
    Student s14 = new Student("a", mark++);
    Student s15 = new Student("a", mark++);
    Student s16 = new Student("a", mark++);

    List<Student> students = Stream.of(s1, s11, s12, s13, s14, s15, s16).collect(Collectors.toList());
    for (Student s : students) {
      Long now = System.currentTimeMillis();
      ctx.collectWithTimestamp(s, now);
      ctx.emitWatermark(new Watermark(now));
      Thread.sleep(500);
    }
  }

  @Override
  public void cancel() {

  }
}
