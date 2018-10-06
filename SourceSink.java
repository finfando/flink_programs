package eit_group;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SourceSink {
    public static void main(String[] args) throws Exception {
        // Program Starts
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String inFilePath = "/home/data-in/test_data.csv";
        String outFilePath = "/home/data-out/test_out.txt";
        DataStreamSource<String> source = env.readTextFile(inFilePath);
        source.writeAsText(outFilePath, FileSystem.WriteMode.OVERWRITE);
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
        // Program Ends
    }
}
