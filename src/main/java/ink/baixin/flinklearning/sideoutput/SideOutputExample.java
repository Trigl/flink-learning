package ink.baixin.flinklearning.sideoutput;

import ink.baixin.flinklearning.wordcount.SocketWindowWordCount;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * <p>This program connects to a server socket and reads strings from the socket. The easiest way to
 * try this out is to open a text server (at port 12345) using the <i>netcat</i> tool via
 *
 * <pre>
 * nc -l 12345 on Linux or nc -l -p 12345 on Windows
 * </pre>
 *
 * <p>and run this example with the hostname and the port as arguments.
 */
public class SideOutputExample {
    private static final OutputTag<SocketWindowWordCount.WordWithCount> overFiveTag = new OutputTag<SocketWindowWordCount.WordWithCount>("overFive") {
    };
    private static final OutputTag<SocketWindowWordCount.WordWithCount> equalFiveTag = new OutputTag<SocketWindowWordCount.WordWithCount>("equalFive") {
    };

    public static void main(String[] args) throws Exception {
        // the host and the port to connect to
        final String hostname;
        final int port;
        try {
            final ParameterTool params = ParameterTool.fromArgs(args);
            hostname = params.has("hostname") ? params.get("hostname") : "localhost";
            port = params.has("port") ? params.getInt("port") : 12345;
        } catch (Exception e) {
            System.err.println(
                    "No port specified. Please run 'SocketWindowWordCount "
                            + "--hostname <hostname> --port <port>', where hostname (localhost by default) "
                            + "and port is the address of the text server");
            System.err.println(
                    "To start a simple text server, run 'netcat -l <port>' and "
                            + "type the input text into the command line");
            return;
        }

        // get the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // get input data by connecting to the socket
        DataStream<String> text = env.socketTextStream(hostname, port, "\n");

        // parse the data, group it, window it, and aggregate the counts
        SingleOutputStreamOperator<SocketWindowWordCount.WordWithCount> tokenizer =
                text.flatMap(
                        new FlatMapFunction<String, SocketWindowWordCount.WordWithCount>() {
                            @Override
                            public void flatMap(
                                    String value, Collector<SocketWindowWordCount.WordWithCount> out) {
                                for (String word : value.split("\\s")) {
                                    out.collect(new SocketWindowWordCount.WordWithCount(word, 1L));
                                }
                            }
                        })
                        .process(new Tokenizer());

        //将字符串长度大于 5 的打印出来
        tokenizer.getSideOutput(overFiveTag).print();

        //将字符串长度等于 5 的打印出来
        tokenizer.getSideOutput(equalFiveTag).print();

        //这个打印出来的是字符串长度小于 5 的，并会累加 count
        tokenizer.keyBy(value -> value.word)
                .reduce(
                        new ReduceFunction<SocketWindowWordCount.WordWithCount>() {
                            @Override
                            public SocketWindowWordCount.WordWithCount reduce(SocketWindowWordCount.WordWithCount a, SocketWindowWordCount.WordWithCount b) {
                                return new SocketWindowWordCount.WordWithCount(a.word, a.count + b.count);
                            }
                        })
                .print();

        env.execute("Streaming WordCount SideOutput");
    }

    public static final class Tokenizer extends ProcessFunction<SocketWindowWordCount.WordWithCount, SocketWindowWordCount.WordWithCount> {
        private static final long serialVersionUID = 1L;

        @Override
        public void processElement(SocketWindowWordCount.WordWithCount value, Context ctx, Collector<SocketWindowWordCount.WordWithCount> out) throws Exception {
            String content = value.word;
            if (content.length() > 5) {
                value.word = value.word + ">5";
                ctx.output(overFiveTag, value); //将字符串长度大于 5 的 word 放到 overFiveTag 中去
            } else if (content.length() == 5) {
                value.word = value.word + "=5";
                ctx.output(equalFiveTag, value); //将字符串长度等于 5 的 word 放到 equalFiveTag 中去
            } else if (content.length() < 5) {
                out.collect(value);
            }

        }
    }
}
