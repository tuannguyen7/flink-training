package org.apache.flink.training.experiment;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class Experiment1 {

  public static void main(String[] args) throws Exception {
    new Experiment1().collectionStream();
  }

  public void collectionStream() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    env.setParallelism(4);
    List<String> strings = Stream.of("a", "b", "c", "a", "b", "a").collect(Collectors.toList());
    List<Integer> numbers = Stream.of(1, 2, 3, 4, 5).collect(Collectors.toList());
    DataStreamSource<String> stringDataStream = env.fromCollection(strings);
    DataStreamSource<Integer> numberDataStream = env.fromCollection(numbers);

    DataStream<String> ds = stringDataStream
          .map((MapFunction<String, String>) m -> m);
          //.keyBy((KeySelector<String, String>) m -> m);
    ds.print();

    env.execute();
  }

  public static class EnrichmentFunction extends
      RichCoFlatMapFunction<String, Integer, Tuple2<String, Integer>> {

    List<String> strings = new ArrayList<>();
    List<Integer> numbers = new ArrayList<>();

    @Override
    public void flatMap1(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
      strings.add(value);
    }

    @Override
    public void flatMap2(Integer value, Collector<Tuple2<String, Integer>> out) throws Exception {
      numbers.add(value);
    }
  }

  public void socketStreamTest() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    DataStream<Tuple2<String, Integer>> dataStream = env
        .socketTextStream("localhost", 9999)
        .flatMap(new Splitter())
        .keyBy(value -> value.f0)
        .timeWindow(Time.seconds(5))
        .sum(1);

    dataStream.print();
    env.execute("Window WordCount");
  }

  public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
    @Override
    public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
      for (String word: sentence.split(" ")) {
        out.collect(new Tuple2<String, Integer>(word, 1));
      }
    }
  }
}
