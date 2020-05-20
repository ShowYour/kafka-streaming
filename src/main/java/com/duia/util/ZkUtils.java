package com.duia.util;

import org.apache.zookeeper.*;
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
    public static void createPath(ZooKeeper zooKeeper,String path, Object data) throws KeeperException, InterruptedException {
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
            if (zooKeeper.exists(buffer.toString(),null)==null){
                zooKeeper.delete(buffer.toString(),-1);
                zooKeeper.create(buffer.toString(),
                        d,
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        }
    }

    /**
     * @param zooKeeper
     * @param path
     * @param watcher
     * @param stat
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    public static byte[] getData(ZooKeeper zooKeeper, String path, Watcher watcher, Stat stat) throws KeeperException, InterruptedException {
        return zooKeeper.getData(path, watcher, stat);
    }

    /**
     * @param zooKeeper
     * @param path
     * @param data
     * @param version
     * @throws KeeperException
     * @throws InterruptedException
     */
    public static void setData(ZooKeeper zooKeeper,String path,byte[] data,int version) throws KeeperException, InterruptedException {
        zooKeeper.setData(path,data,version);
    }
}
