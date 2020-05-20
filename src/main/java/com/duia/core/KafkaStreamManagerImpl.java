package com.duia.core;

import com.duia.util.ZkUtils;
import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.support.PropertiesLoaderUtils;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import com.duia.core.KafkaStreamApplication.KafkaStreamApplicationStat;

/**
 * @ClassName KafkaStreamManagerImpl
 * @Author xiaoyu
 * @Date 2020/5/15 18:37
 * @Description TODO
 **/
public class KafkaStreamManagerImpl implements KafkaStreamManager {
    private static final Logger LOGGER =  LoggerFactory.getLogger(KafkaStreamManagerImpl.class);
    private static final String rootPath = "/kafkaStream/application/";
    private static ZooKeeper zooKeeper;
    public KafkaStreamManagerImpl(){
        zooKeeper = createZookeeper();
    }

    @Override
    public boolean startApplication(KafkaStreamApplication application) {
        if (application.getStat()== KafkaStreamApplication.KafkaStreamApplicationStat.STOP){
            //启动成功后，设置状态并通知zookeeper
            try {
                application.start();
                ZkUtils.setData(getZooKeeper(),rootPath+application.getApplicationName(),
                        KafkaStreamApplicationStat.START.toString().getBytes(),-1);
                return true;
            } catch (KeeperException | InterruptedException e) {
                LOGGER.error("{}",e);
            }
        }
        return false;
    }

    @Override
    public boolean stopApplication(KafkaStreamApplication application) {
        if (application.getStat()== KafkaStreamApplication.KafkaStreamApplicationStat.START){
            //停止成功后，通知zookeeper
            try {
                application.stop();
                getZooKeeper().setData(rootPath+application.getApplicationName(),
                        KafkaStreamApplicationStat.STOP.toString().getBytes(),-1);
                return true;
            } catch (KeeperException |InterruptedException e) {
                LOGGER.error("{}",e);
            }
        }
        return false;
    }

    @Override
    public boolean registerKafkaStreamApplication(KafkaStreamApplication application) {
        try {
            if (application.getStat()==KafkaStreamApplicationStat.START){
                application.start();
            }
            ZkUtils.createPath(getZooKeeper(),rootPath+application.getApplicationName(),application.getStat().toString());
        } catch (KeeperException | InterruptedException e) {
            LOGGER.error("注册kafkaStreamApplication失败,error:{}",e);
        }
        return true;
    }

    @Override
    public void watchKafkaStreamApplication(KafkaStreamApplication application) {
        try {
            ZkUtils.getData(getZooKeeper(),rootPath + application.getApplicationName(),
                    new KafkaStreamWatcher(this,application),null);
        } catch (KeeperException | InterruptedException e) {
            LOGGER.error("监听kafkaStreamApplication在zookeeper上的状态失败,error:{}",e);
        }
    }

    /**
    *@ClassName KafkaStreamManagerImpl
    *@Author xiaoyu
    *@Date 2020/5/19 11:00
    *@Description TODO
    **/
    private static class KafkaStreamWatcher implements Watcher{
        private KafkaStreamApplication application;
        private KafkaStreamManager kafkaStreamManager;
        public KafkaStreamWatcher(KafkaStreamManager kafkaStreamManager,KafkaStreamApplication application){
            this.application = application;
            this.kafkaStreamManager = kafkaStreamManager;
        }

        @Override
        public void process(WatchedEvent event) {
            try {
                byte[] data = ZkUtils.getData(getZooKeeper(), rootPath + application.getApplicationName(), this, null);
                KafkaStreamApplicationStat stat = KafkaStreamApplicationStat.valueOf(new String(data));
                switch (stat){
                    case STOP:
                        kafkaStreamManager.stopApplication(application);
                        break;
                    case START:
                        kafkaStreamManager.startApplication(application);
                        break;
                    default:
                        break;
                }
            } catch (KeeperException | InterruptedException e) {
                LOGGER.error("{}",e);
            }
        }
    }

    /**
     * @return 创建zookeeper
     */
    private static ZooKeeper createZookeeper(){
        ZooKeeper zooKeeper = null;
        CountDownLatch countDownLatch = new CountDownLatch(1);
        try {
            Properties properties = PropertiesLoaderUtils.loadAllProperties("application.properties");
            zooKeeper = new ZooKeeper(properties.getProperty("zookeeper.url"), 3000, event -> {
                if (event.getState()== Watcher.Event.KeeperState.SyncConnected){
                    countDownLatch.countDown();
                }
            });
            countDownLatch.await();
        } catch (IOException | InterruptedException e) {
            LOGGER.error("创建zookeeper客户端失败",e);
        }
        return zooKeeper;
    }

    /**
     * 判断zookeeper是否可用，若不可用则创建一个
     * @return
     */
    private static ZooKeeper getZooKeeper(){
        if (zooKeeper==null || !zooKeeper.getState().isAlive()){
            synchronized (KafkaStreamManagerImpl.class){
                if (zooKeeper==null || !zooKeeper.getState().isAlive()){
                    zooKeeper = createZookeeper();
                }
            }
        }
        return zooKeeper;
    }
}
