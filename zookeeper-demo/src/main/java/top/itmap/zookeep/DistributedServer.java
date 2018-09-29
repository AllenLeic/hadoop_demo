package top.itmap.zookeep;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.UUID;

/**
 * @Description: 基于zookeeper 的分布式系统的上下线动态感知 服务端
 * @Author: AllenLei (leicong@foxmail.com)
 * @date: 2018/09/29
 */
public class DistributedServer {

    // 会话超时时间
    public static final int SESSION_TIMEOUT = 3000;
    // zookeeper 连接信息
    private static final String connectString = "mini1:2181,mini2:2181,mini3:2181";
    // 节点父路径
    private static final String PARENT_PATH = "/servers";

    ZooKeeper zk;

    /**
     * 获取zookeeper 连接
     *
     * @throws IOException
     */
    private void getZkConnect() throws IOException {
        zk = new ZooKeeper(connectString, SESSION_TIMEOUT, event -> {
            // 收到事件通知后的回调函数（应该是我们自己要处理的事件）
            System.out.println(event.getType() + "---" + event.getPath());
            try {
                zk.getChildren("/", true);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }


    /**
     * 注册到zookeeper上面
     *
     * @param hostname 主机名
     */
    private void regesterServer(String hostname) throws Exception {
        // 如果父节点不存在就创建一个
        if (zk.exists(PARENT_PATH, false) == null) {
            zk.create(PARENT_PATH, UUID.randomUUID().toString().getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        // 创建节点
        String create = zk.create(PARENT_PATH + "/server", hostname.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println(hostname + "is online.." + create);

    }

    public static void main(String[] args) throws Exception {

        // 1. 获取zookeeper 的连接
        DistributedServer server = new DistributedServer();
        server.getZkConnect();
        // 2. 服务端注册到zookeeper 上面
        server.regesterServer(args[0]);
        // 3. 服务端的业务功能
        server.handleBussiness(args[0]);
    }

    /**
     * 业务功能
     *
     * @param arg
     */
    private void handleBussiness(String arg) throws InterruptedException {
        System.out.println(arg + "is working.....");
        Thread.sleep(Long.MAX_VALUE);
    }
}
