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

package ink.baixin.flinklearning.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;
import org.apache.flink.streaming.api.functions.source.datagen.RandomGenerator;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaSerializationSchemaWrapper;

import java.util.Properties;

@SuppressWarnings("serial")
@Slf4j
public class RandomData2KafkaExample {

    public static void main(String[] args) throws Exception {

        // the host and the port to connect to
        final ParameterTool params = ParameterTool.fromArgs(args);
        final String brokerlist = params.has("broker-list") ? params.get("broker-list") : "localhost:9092";
        final String topic = params.has("topic") ? params.get("topic") : "source-topic";

        // get the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", brokerlist);

        DataStream<String> stream = env
                .addSource(new DataGeneratorSource<>(RandomGenerator.stringGenerator(10), 10000, null))
                .returns(Types.STRING);

        FlinkKafkaProducer<String> myProducer = new FlinkKafkaProducer<>(
                topic,
                new KafkaSerializationSchemaWrapper(topic, null, false, new SimpleStringSchema()),
                properties,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE);

        stream.addSink(myProducer).name("Kafka2Kafka Example");

        env.execute("Kafka Example");
    }

}
