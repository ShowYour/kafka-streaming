package com.duia.core;

import org.apache.kafka.streams.Topology;
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
    void start();

    /**
     * 关闭application
     */
    void stop();

    /**
     * @return 返回application的状态
     */
    KafkaStreamApplicationStat getStat();

    /**
     * @param stat 设置application的状态
     */
    void setStat(KafkaStreamApplicationStat stat);
}
