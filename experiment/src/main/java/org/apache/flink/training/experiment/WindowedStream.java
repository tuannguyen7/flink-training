package org.apache.flink.training.experiment;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.training.experiment.generators.SingleStudentGenerator;
import org.apache.flink.training.experiment.pojo.Student;

public class WindowedStream {

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

    DataStream<Student> studentDataStream =
        env.addSource(new SingleStudentGenerator())
            .keyBy((KeySelector<Student, String>) s -> s.name)
            //.window(TumblingEventTimeWindows.of(Time.seconds(5)))
            .maxBy("mark");
            //.max("mark");
            //.window(TumblingEventTimeWindows.of(Time.seconds(10)))


    studentDataStream.print();
    env.execute();
  }
}
