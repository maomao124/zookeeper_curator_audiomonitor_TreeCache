import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Scanner;

/**
 * Project name(项目名称)：zookeeper_curator监听器之TreeCache
 * Package(包名): PACKAGE_NAME
 * Class(类名): ZooKeeper
 * Author(作者）: mao
 * Author QQ：1296193245
 * GitHub：https://github.com/maomao124/
 * Date(创建日期)： 2022/4/20
 * Time(创建时间)： 22:58
 * Version(版本): 1.0
 * Description(描述)： 无
 */

public class ZooKeeper
{
    private CuratorFramework client;

    @BeforeEach
    void setUp()
    {
        //重试策略
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(3000, 10);
        //zookeeper创建链接，第一种
                        /*
                        CuratorFramework client =
                                CuratorFrameworkFactory.newClient("127.0.0.1:2181",
                                        60 * 1000,
                                        15 * 1000,
                                        retryPolicy);
                        client.start();
                        */

        //zookeeper创建链接，第二种
        client = CuratorFrameworkFactory.builder()
                .connectString("127.0.0.1:2181")
                .sessionTimeoutMs(60 * 1000)
                .connectionTimeoutMs(15 * 1000)
                .retryPolicy(retryPolicy)
                .namespace("test")
                .build();
        client.start();
    }

    @AfterEach
    void tearDown()
    {
        if (client != null)
        {
            client.close();
        }
    }

    @Test
    void test1() throws Exception
    {
        TreeCache treeCache = new TreeCache(client, "/app4");
        treeCache.getListenable().addListener(new TreeCacheListener()
        {
            @Override
            public void childEvent(CuratorFramework curatorFramework, TreeCacheEvent treeCacheEvent) throws Exception
            {
                System.out.println("节点和子节点已改变");
                System.out.println(treeCacheEvent);
                byte[] data = treeCacheEvent.getData().getData();
                System.out.println("改变：" + new String(data, StandardCharsets.UTF_8));
                TreeCacheEvent.Type type = treeCacheEvent.getType();
                if (type.equals(TreeCacheEvent.Type.NODE_UPDATED))
                {
                    System.out.println("是更新");
                }
            }
        });

        Scanner input = new Scanner(System.in);
        treeCache.start();
        for (int i = 0; i < 3; i++)
        {
            input.nextLine();
        }
    }
}
