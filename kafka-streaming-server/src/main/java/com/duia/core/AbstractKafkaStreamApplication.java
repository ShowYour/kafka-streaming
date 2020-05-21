package com.duia.core;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;

/**
 * @ClassName AbstractKafkaStreamApplication
 * @Author xiaoyu
 * @Date 2020/5/18 15:53
 * @Description TODO
 **/

public abstract class AbstractKafkaStreamApplication implements KafkaStreamApplication {
    protected final Logger LOGGER = LoggerFactory.getLogger(this.getClass());
    private KafkaStreams streams;

    public abstract Properties getConfiguration();

    public abstract Topology getTopology();

    @Override
    public String getApplicationName() {
        return this.getClass().getSimpleName();
    }

    @Override
    public final synchronized void start() throws IllegalStateException,StreamsException{
        Topology topology = getTopology();
        Properties props = getConfiguration();
        if (streams==null || !streams.state().isRunning()) {
            streams = new KafkaStreams(topology, props);
        }
        streams.start();
    }

    @Override
    public final synchronized void stop() {
        if (streams!=null){
            streams.close();
            streams = null;
        }
    }

    @Override
    public final boolean isAlive() {
        if (streams!=null && streams.state().isRunning()){
            return true;
        }else {
            stop();
            return false;
        }
    }
}
