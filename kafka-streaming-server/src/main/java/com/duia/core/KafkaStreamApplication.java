package com.duia.core;

import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsException;

import java.util.Properties;

/**
*@ClassName KafkaStreamApplication
*@Author xiaoyu
*@Date 2020/5/15 16:15
*@Description TODO kafka stream 接口类
**/
public interface KafkaStreamApplication {
    /**
     * kafkaStream application的状态
     */
    enum KafkaStreamApplicationStat{
        START,STOP
    }

    /**
     * @return 获取创建kafka application configuration
     */
    Properties getConfiguration();

    /**
     * @return 获取kafkaStream执行逻辑
     */
    Topology getTopology();

    /**
     * @return 获取kafka application的名称 唯一
     */
    String getApplicationName();

    /**
     * 启动application
     */
    void start() throws IllegalStateException,StreamsException;

    /**
     * 关闭application
     */
    void stop();

    /**
     * kafkaStreamApplication是否存活
     * @return
     */
    boolean isAlive();
}
