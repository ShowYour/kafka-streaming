package com.duia.core;

/**
*@ClassName KafkaStreamManager
*@Author xiaoyu
*@Date 2020/5/15 16:20
*@Description TODO kafka stream application 管理接口
**/
public interface KafkaStreamManager {

    /**
     * 启动一个application
     *
     * @param application
     * @return
     */
    boolean startApplication(KafkaStreamApplication application);

    /**
     * 关闭一个application
     *
     * @param application
     * @return
     */
    boolean stopApplication(KafkaStreamApplication application);


    /**
     * 将application注册到分布式系统上，等待启动或者停止操作
     * @param application
     * @return
     */
    boolean registerKafkaStreamApplication(KafkaStreamApplication application);

    /**
     * 若该application已注册，则监听它的启动停止状态
     * @param application
     */
    void watchKafkaStreamApplication(KafkaStreamApplication application);

}