package top.itmap.zookeep;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

/**
 * @Description: zookeeper api 的练习
 * @Author: AllenLei (leicong@foxmail.com)
 * @date: 2018/09/26
 */
public class SimpleDemo {
    // 会话超时时间
    public static final int SESSION_TIMEOUT = 3000;

    /**
     * 连接字符串 connectString 如果是集群则按下面格式给出
     * 格式为 "server1:port,server2:port,server3:port
     */
    private static final String connectString = "mini1:2181,mini2:2181,mini3:2181";

    // 创建实例
    ZooKeeper zkClient;

    // 创建 watch 实例
    Watcher watcher = new Watcher() {
        public void process(WatchedEvent event) {
            // 收到事件通知后的回调函数（应该是我们自己要处理的事件）
            System.out.println(event.getType() + "---" + event.getPath());
            try {
                zkClient.getChildren("/", true);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    };

    /**
     * 初始化 zookeeper
     *
     * @throws Exception
     */
    @Before
    public void initZookeeper() throws Exception {
        // 参数1：连接字符串。参数2：会话超时时间。 参数3：监听器
        zkClient = new ZooKeeper(connectString, SESSION_TIMEOUT, watcher);
    }

    // 获取子节点
    @Test
    public void getChildern() throws Exception {
        List<String> children = zkClient.getChildren("/", true);
        for (String child : children) {
            System.out.println(child);
        }

        Thread.sleep(Integer.MAX_VALUE);
    }

    /**
     * 创建节点
     *
     * @throws KeeperException
     * @throws InterruptedException
     */
    @Test
    public void testCreate() throws KeeperException, InterruptedException {
        // 参数1：要创建的节点路径 参数2：节点数据  参数3：节点权限 参数4：节点的类型
        String nodeCreate = zkClient.create("/idea2018", "hellozk".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        // 节点数据可以是任何数据。
    }

    /**
     * 判断节点是否存在
     *
     * @throws Exception
     */
    @Test
    public void testExist() throws Exception {
        Stat stat = zkClient.exists("/idea2018", true);
        System.out.println(stat == null ? "not exist" : "exist");

    }

    /**
     * 获取数据
     *
     * @throws Exception
     */
    @Test
    public void testGetData() throws Exception {
        byte[] data = zkClient.getData("/idea2018", false, null);
        System.out.println(new String(data, "UTF-8"));
    }

    /**
     * 修改数据
     *
     * @throws Exception
     */
    @Test
    public void testSetData() throws Exception {
        // 参数1：路径，参数2：数据，参数3：版本号， 1- 代表任何版本
        zkClient.setData("/idea2018", "hello-idea".getBytes(), -1);
    }

    /**
     * 删除节点
     *
     * @throws Exception
     */
    @Test
    public void testDeleteZnode() throws Exception {
        // 参数1：路径 参数2：版本号：-1 表示删除所有版本
        zkClient.delete("/idea2018", -1);
    }
}
