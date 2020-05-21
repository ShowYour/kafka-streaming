package com.duia.util;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName ZkUtils
 * @Author xiaoyu
 * @Date 2020/5/20 15:34
 * @Description TODO
 **/
public final class ZkUtils {

    /**
     * zookeeper创建节点，若父节点不存在则先创建父节点 /a/b/c
     * @param path
     * @param data
     * @throws KeeperException
     * @throws InterruptedException
     */
    public static void createPath(ZooKeeper zooKeeper,String path, Object data) {
        List<String> newPaths = new ArrayList<>();
        char[] chars = path.toCharArray();
        StringBuffer stringBuffer = new StringBuffer();
        for (int i = 0; i < chars.length; i++) {
            if (chars[i]=='/'){
                String newPath = stringBuffer.toString();
                if (newPath!=null && newPath.trim().length()>0){
                    newPaths.add(newPath);
                }
                stringBuffer = new StringBuffer();
            }
            stringBuffer.append(chars[i]);
        }

        if (stringBuffer.toString()!=null && stringBuffer.toString().trim().length()>0){
            newPaths.add(stringBuffer.toString());
        }

        for (int i = 0; i < newPaths.size(); i++) {
            List<String> newPath = newPaths.subList(0, i+1);
            StringBuffer buffer = new StringBuffer();
            for (String s:newPath){
                buffer.append(s);
            }
            byte[] d = (i==newPaths.size()-1)?data.toString().getBytes():null;
            if (exists(zooKeeper,buffer.toString(),null)==null){
                create(zooKeeper,buffer.toString(),d,null,CreateMode.PERSISTENT);
            }else {
                setData(zooKeeper,buffer.toString(),d,-1);
            }
        }
    }

    /**
     * @param zooKeeper
     * @param path
     * @param data
     * @param ids
     * @param createMode
     * @return
     */
    public static String create(ZooKeeper zooKeeper, String path, byte[] data, List<ACL> ids,CreateMode createMode){
        String nodePath = null;
        try {
            nodePath = zooKeeper.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return nodePath;
    }
    /**
     * @param zooKeeper
     * @param path
     * @param watcher
     * @param stat
     * @return
     */
    public static byte[] getData(ZooKeeper zooKeeper, String path, Watcher watcher, Stat stat) {
        byte[] data = new byte[0];
        try {
            data = zooKeeper.getData(path, watcher, stat);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return data;
    }

    /**
     * @param zooKeeper
     * @param path
     * @param data
     * @param version
     */
    public static void setData(ZooKeeper zooKeeper,String path,byte[] data,int version) {
        try {
            zooKeeper.setData(path,data,version);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * @param zooKeeper
     * @param path
     * @param watcher
     * @return
     */
    public static Stat exists(ZooKeeper zooKeeper,String path,Watcher watcher){
        Stat stat = null;
        try {
            stat = zooKeeper.exists(path,watcher);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return stat;
    }
}
