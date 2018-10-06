package eit_group;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class FlatMap {
    public static void main(String[] args) throws Exception {
        // Program Starts
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String inFilePath = "/home/data-in/test_data.csv";
        String outFilePath = "/home/data-out/test_out.txt";
        DataStreamSource<String> source = env.readTextFile(inFilePath);

        SingleOutputStreamOperator<Tuple3<Long, String, Double>> mapStream = source.map(
                new MapFunction<String, Tuple3<Long, String, Double>>() {
                    @Override
                    public Tuple3<Long, String, Double> map(String in) throws Exception {
                        String[] fieldArray = in.split(",");
                        Tuple3<Long, String, Double> out = new Tuple3(Long.parseLong(fieldArray[0]), fieldArray[1], Double.parseDouble(fieldArray[2]));
                        return out;
                    }
                }
        );

        SingleOutputStreamOperator<Tuple3<Long, String, Double>> flatMapOut = mapStream.flatMap(
                new FlatMapFunction<Tuple3<Long, String, Double>, Tuple3<Long, String, Double>>() {
                    @Override
                    public void flatMap(Tuple3<Long, String, Double> in, Collector<Tuple3<Long, String, Double>> out)
                            throws Exception {
                        if (in.f1.equals("sensor1")) {
                            out.collect(in);
                            out.collect(new Tuple3<Long, String, Double>(in.f0, in.f1, 100.10));
                        }
                }
        });
        flatMapOut.writeAsCsv(outFilePath, FileSystem.WriteMode.OVERWRITE);
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
        // Program Ends
    }
}
