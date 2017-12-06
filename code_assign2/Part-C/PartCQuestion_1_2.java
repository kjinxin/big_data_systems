package org.myorg.quickstart;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.TimeCharacteristic;

import java.io.*;  

/**
* Main func
*/
public class PartCQuestion_1_2 {

	public static void main(String[] args) throws Exception {

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		//DataStream<Tuple5<Integer, Integer, Long, String, Integer>> stream = env.addSource(DataSource().create());
		DataStream<Tuple4<Integer, Integer, Long, String>> stream = env.addSource(DataSource.create());
		//stream.assignTimestampsAndWatermarks(new TimestampExtractor());
		DataStream<String> output_sliding=
                        stream.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple4<Integer, Integer, Long, String>>() {
                        @Override
                        public long extractAscendingTimestamp(Tuple4<Integer, Integer, Long, String> element) {
                                return element.f2*1000L;
                            }
                        })
			.keyBy(3)
                        .window(SlidingEventTimeWindows.of(Time.minutes(1), Time.seconds(1)))
                        .apply(new MyWindowFunction());
                output_sliding.writeAsText("/home/ubuntu/hw2_partc_dir/part_c_out_sliding");

		env.execute("Part-C Q1 Trial!");
	}

private static class TimestampExtractor implements AssignerWithPunctuatedWatermarks<Tuple4<Integer, Integer, Long, String>> {

	@Override
	public long extractTimestamp(Tuple4<Integer, Integer, Long, String> element, long previousElementTimestamp) {
		return element.f2*1000L;
	}

	@Override
	public Watermark checkAndGetNextWatermark(Tuple4<Integer, Integer, Long, String> lastElement, long extractedTimestamp) {
	//	return lastElement.hasWatermarkMarker() ? new Watermark(extractedTimestamp) : null;
		return new Watermark(extractedTimestamp);
	}
}

private static class MyWindowFunction implements WindowFunction<Tuple4<Integer, Integer, Long, String>, String, Tuple, TimeWindow> {

 public void apply(Tuple key, TimeWindow window, Iterable<Tuple4<Integer, Integer, Long, String>> input, Collector<String> out) {
    long count = 0;
    for (Tuple4<Integer, Integer, Long, String> in: input) {
      count++;
    }
//    out.collect("TimeWindow{start=" + window.getStart() + ", end=" + window.getEnd() + " Count: " + count + " Type: " + key);
      if(count >= 100){
         out.collect("TimeWindow{start=" + window.getStart() + ", end=" + window.getEnd() + "} Count: " + count + " Type: " + key.getField(0));}
   }
}

/**
* streaming simulation part
*/
private static class DataSource extends RichSourceFunction<Tuple4<Integer, Integer, Long, String>> {

        private volatile boolean running = true;
        private final String filename = "/home/ubuntu/higgs-activity_time.txt";

        private DataSource() {

        }

        public static DataSource create() {
                return new DataSource();
        }

        @Override
        public void run(SourceContext<Tuple4<Integer, Integer, Long, String>> ctx) throws Exception {

                try{
                        final File file = new File(filename);
			//System.out.println("Here !=====================");
                        final BufferedReader br = new BufferedReader(new FileReader(file));

                        String line = "";

                        System.out.println("Start read data from \"" + filename + "\"");
                        long count = 0L;
                        while(running && (line = br.readLine()) != null) {
                                if ((count++) % 10 == 0) {
                                        Thread.sleep(1);
                                }
                                ctx.collect(genTuple(line));
                        }
		  }
                catch (Exception e) {
		    	//System.out.print("Exception: ");
        		//System.out.println(e.getMessage());
                       e.printStackTrace();
                }
        }

        @Override
        public void cancel() {
                running = false;
        }

        private Tuple4<Integer, Integer, Long, String> genTuple(String line) {
                String[] item = line.split(" ");
                Tuple4<Integer, Integer, Long, String> record = new Tuple4<>();

                record.setField(Integer.parseInt(item[0]), 0);
                record.setField(Integer.parseInt(item[1]), 1);
                record.setField(Long.parseLong(item[2]), 2);
                record.setField(item[3], 3);

                return record;
        }
}
}
