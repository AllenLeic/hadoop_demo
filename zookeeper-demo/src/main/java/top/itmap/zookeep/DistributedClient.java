package top.itmap.zookeep;

import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @Description: 基于zookeeper 的分布式系统的上下线动态感知 客户端
 * @Author: AllenLei (leicong@foxmail.com)
 * @date: 2018/09/29
 */
public class DistributedClient {

    // 会话超时时间
    public static final int SESSION_TIMEOUT = 3000;
    // zookeeper 连接信息
    private static final String connectString = "mini1:2181,mini2:2181,mini3:2181";
    // 节点父路径
    private static final String PARENT_PATH = "/servers";
    // 服务器上线的节点
    private volatile ArrayList<String> serverList;

    ZooKeeper zk = null;

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
                getServerList();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    /**
     * 发现服务
     */
    private void getServerList() throws Exception {
        // 获取节点名 这里需要监听，因为会看到服务器的上下前动态变化
        List<String> children = zk.getChildren(PARENT_PATH, true);
        // 获取节点数据
        ArrayList<String> servers = new ArrayList<>();
        for (String child: children) {
            // 这里不watch 是因为 watch 的是 子节点数据变化的事件
            byte[] data = zk.getData(PARENT_PATH + "/" + child, false, null);
            servers.add(new String(data));
        }
        serverList = servers;
        System.out.println(serverList);

    }

    /**
     * 业务功能
     *
     */
    private void handleBussiness() throws InterruptedException {
        System.out.println("client is working.....");
        Thread.sleep(Long.MAX_VALUE);
    }

    public static void main(String[] args) throws Exception {
        DistributedClient client = new DistributedClient();
        // 1. 获取 zookeeper 连接
        client.getZkConnect();
        // 2. 发现服务并监听服务的上下线
        client.getServerList();
        // 3. 业务功能
        client.handleBussiness();
    }
}
