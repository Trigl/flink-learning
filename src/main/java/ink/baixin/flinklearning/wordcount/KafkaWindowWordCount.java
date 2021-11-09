/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ink.baixin.flinklearning.wordcount;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * Implements a Kafka streaming windowed version of the "WordCount" program.
 *
 * <p>This program consume String format data from Kafka. The easiest way to
 * try this out is to set up a local Kafka Server by docker-compose.
 * Run following command in `resources` dir:
 * <pre>
 * docker-compose up -d
 * </pre>
 */
@SuppressWarnings("serial")
@Slf4j
public class KafkaWindowWordCount {

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);
        final String brokerlist = params.has("broker-list") ? params.get("broker-list") : "localhost:9092";
        final String topic = params.has("topic") ? params.get("topic") : "flink-learning";
        final String groupId = params.has("groupId") ? params.get("groupId") : "flink-test";

        // get the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", brokerlist);
        properties.setProperty("group.id", groupId);

        DataStream<String> stream = env.addSource(new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties));

        // parse the data, group it, window it, and aggregate the counts
        DataStream<WordWithCount> windowCounts =
                stream.flatMap(
                                new FlatMapFunction<String, WordWithCount>() {
                                    @Override
                                    public void flatMap(
                                            String value, Collector<WordWithCount> out) {
                                        for (String word : value.split("\\s")) {
                                            out.collect(new WordWithCount(word, 1L));
                                        }
                                    }
                                })
                        .keyBy(value -> value.word)
                        .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                        .reduce(
                                new ReduceFunction<WordWithCount>() {
                                    @Override
                                    public WordWithCount reduce(WordWithCount a, WordWithCount b) {
                                        return new WordWithCount(a.word, a.count + b.count);
                                    }
                                });

        // print the results with a single thread, rather than in parallel
        windowCounts.print().setParallelism(1);

        env.execute("Socket Window WordCount");
    }

    // ------------------------------------------------------------------------

    /** Data type for words with count. */
    public static class WordWithCount {

        public String word;
        public long count;

        public WordWithCount() {}

        public WordWithCount(String word, long count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return word + " : " + count;
        }
    }
}
