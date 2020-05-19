package com.duia.kafkaStreamAplications;

import com.duia.core.AbstractKafkaStreamApplication;
import com.duia.core.KafkaStream;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import java.util.Properties;

/**
 * @ClassName kafkaApplicationDemo
 * @Author xiaoyu
 * @Date 2020/5/18 10:16
 * @Description TODO
 **/
@KafkaStream
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
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> source = builder.stream("streams-plaintext-input");
        source.to("streams-pipe-output");
        Topology topology = builder.build();
        return topology;
    }

}
