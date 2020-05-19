package com.duia.core;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
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
    private volatile KafkaStreamApplicationStat stat = KafkaStreamApplicationStat.STOP;
    private KafkaStreams streams;

    public abstract Properties getConfiguration();

    public abstract Topology getTopology();

    @Override
    public String getApplicationName() {
        return this.getClass().getSimpleName();
    }

    @Override
    public final synchronized boolean start() {
        Topology topology = getTopology();
        Properties props = getConfiguration();
        if (streams==null) {
            streams = new KafkaStreams(topology, props);
        }
        streams.start();
        setStat(KafkaStreamApplicationStat.START);
        return false;
    }

    @Override
    public final synchronized boolean stop() {
        if (streams!=null){
            streams.close();
        }
        setStat(KafkaStreamApplicationStat.STOP);
        return false;
    }

    @Override
    public final synchronized KafkaStreamApplicationStat getStat() {
        return stat;
    }

    @Override
    public final synchronized void setStat(KafkaStreamApplicationStat stat) {
        this.stat = stat;
    }
}
