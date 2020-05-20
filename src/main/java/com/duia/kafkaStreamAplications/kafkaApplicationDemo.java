package com.duia.kafkaStreamAplications;

import com.duia.core.AbstractKafkaStreamApplication;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import java.util.Arrays;
import java.util.Properties;

/**
 * @ClassName kafkaApplicationDemo
 * @Author xiaoyu
 * @Date 2020/5/18 10:16
 * @Description TODO
 **/
public class kafkaApplicationDemo extends AbstractKafkaStreamApplication {

    @Override
    public Properties getConfiguration() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pipe-2");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "bigdata4:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        return props;
    }

    @Override
    public Topology getTopology() {
        Topology topology = simplest("simplest-input", "simplest-output");
        return topology;
    }


    private Topology simplest(String input,String output){
        StreamsBuilder builder = new StreamsBuilder();
        KStream<Object, String> stream = builder.stream(input);
        stream.to(output);
        Topology topology = builder.build();
        return topology;
    }
    /**
     * 单词数量统计
     * @return
     */
    private Topology wordCount(String input,String output){
        StreamsBuilder builder = new StreamsBuilder();
        KStream<Object, String> stream = builder.stream(input);
        KStream<Object, String> words = stream.flatMapValues(value -> Arrays.asList(value.split(",")));
        KGroupedStream<String, String> vGroup = words.groupBy((k, v) -> v);
        KTable<String, Long> count = vGroup.count();
        count.toStream().to(output);
        Topology topology = builder.build();
        return topology;
    }
}
