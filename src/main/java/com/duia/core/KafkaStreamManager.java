package com.duia.core;

import java.util.Collection;

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
    void startApplication(KafkaStreamApplication application);

    /**
     * 关闭一个application
     *
     * @param application
     * @return
     */
    void stopApplication(KafkaStreamApplication application);


    /**
     * 将application注册到分布式系统上，等待启动或者停止操作
     * @param application
     * @return
     */
    void registerKafkaStreamApplication(KafkaStreamApplication application);

    /**
     * 若该application已注册，则监听它的启动停止状态
     * @param application
     */
    void watchKafkaStreamApplication(KafkaStreamApplication application);

    /**
     * 检查kafkaStreamApplication中的stream实例是否处于Alive状态
     * @param apps
     */
    void supervisorKafkaStreamApps(Collection<KafkaStreamApplication> apps);

}