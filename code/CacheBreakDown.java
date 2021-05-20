

import redis.clients.jedis.Jedis;

import java.util.Map;
import java.util.concurrent.*;

/**
 * @author:Li
 * @time: 2019/4/28 10:34
 * @version: 1.0.0
 * 缓存击穿解决方案
 */
public class CacheBreakDown {

    public static void main(String[] args) throws InterruptedException {
        // 从连接池获取jedis
        Jedis jedis = JedisPoolTest.getJedis();
        jedis.del("hot:post:1:value");
        jedis.del("hot:post:2:value");
        jedis.del("hot:post:3:value");

        // 插入过期数据
        jedis.hset("hot:post:2:value", "value", "hot:post:2:value");
        jedis.hset("hot:post:2:value", "timeout", String.valueOf(System.currentTimeMillis() - 10));

         current(5, 1);

        // 启动队列
//        threadPool.execute(() -> execMQ());
//        current(5, 2);
    }

    /**
     * 并发方法
     *
     * @param i 并发几次
     */
    private static void current(int i, int methodId) {
        CyclicBarrier cyclicBarrier = new CyclicBarrier(i);
        for (int j = 0; j < i; j++) {
            new Thread(new Runner(cyclicBarrier, methodId)).start();
        }
    }

    static class Runner implements Runnable {
        CyclicBarrier cyclicBarrier;
        int methodId;

        public Runner(CyclicBarrier cyclicBarrier, int methodId) {
            this.cyclicBarrier = cyclicBarrier;
            this.methodId = methodId;
        }

        @Override
        public void run() {
            try {
                cyclicBarrier.await();
                if (methodId == 1) {
                    System.out.println(Thread.currentThread().getName() + " #" + System.currentTimeMillis() + " #" + exam1(JedisPoolTest.getJedis(), "hot:post:1:value"));
                } else if (methodId == 2) {
                    System.out.println(Thread.currentThread().getName() + " #" + System.currentTimeMillis() + " #" + exam2(JedisPoolTest.getJedis(), "hot:post:2:value"));
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (BrokenBarrierException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 使用互斥锁
     * 该方法是比较普遍的做法, 在根据key获得的value值为空时, 先锁上, 再从数据库加载, 加载完毕, 释放锁, 若其他线程发现获取锁失败, 则睡眠后重试
     * 至于锁的类型, 单机环境用并发包的Lock类型就行, 集群环境则使用分布式锁 (redis的setnx)
     * <p>
     * 优点: 思路简单 保证数据一致性
     * 缺点: 代码复杂度增大  存在死锁概率
     */
    private static String exam1(Jedis jedis, String key) throws InterruptedException {
        // 获取数据
        String s = jedis.get(key);
        if (s == null) {
            // 获得锁
            long lockValue = RedisLock.tryLock(jedis, "lock:break1");
            if (lockValue != 0) {
                System.out.println("by BD");
                // 从数据库查找
                String valueByDB = getValueByDB(key);
                // 插入到redis
                jedis.set(key, valueByDB);
                // 释放锁
                RedisLock.unLock(jedis, "lock:break1", lockValue);
                return valueByDB;
            } else {
                // 没获得到锁 休眠  已经有其他线程在查询数据了
                Thread.sleep(200);
                return exam1(jedis, key);
            }
        } else {
            System.out.println("by Cache");
            return s;
        }
    }

    /**
     * 异步构建缓存
     * 构建缓存采取异步策略, 会从线程池中取线程来异步构建缓存, 从而不会让所有的请求直接怼到数据库上
     * 该方案redis自己维护一个timeout, 当timeout小于System.currentTimeMillis()时, 则进行缓存更新，否则直接返回value值
     * <p>
     * 优点: 体验最佳, 用户无需等待
     * 缺点: 无法保证缓存一致性
     */
    private static String exam2(Jedis jedis, String key) throws InterruptedException {
        // 获取数据
        Map<String, String> map = jedis.hgetAll(key);
        if (Long.valueOf(map.get("timeout")) <= System.currentTimeMillis()) {
            // 需要更新数据 异步后台执行
            threadPool.execute(() -> queue.add(key));
        } else {
            // 无需更新数据
            System.out.println("by Cache in exam2");
        }
        return map.get("value");
    }

    /**
     * 构建线程池
     */
    private static ExecutorService threadPool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

    /**
     * 构建一个队列
     */
    private static LinkedBlockingQueue queue = new LinkedBlockingQueue(255);

    /**
     * 处理消息的队列
     */
    private static void execMQ() {
        Jedis jedis = JedisPoolTest.getJedis();
        for (; ; ) {
            try {
                // 无限处理队列, 如果没有阻塞
                String key = (String) queue.take();
                Map<String, String> map = jedis.hgetAll(key);
                if (map.isEmpty() || Long.valueOf(map.get("timeout")) <= System.currentTimeMillis()) {
                    System.out.println("by MQ DB in execMQ");
                    jedis.hset(key, "value", getValueByDB(key));
                    jedis.hset(key, "timeout", String.valueOf(System.currentTimeMillis() + 2000));
                } else {
                    System.out.println("by Cache in execMQ");
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 模拟数据库查询
     */
    private static String getValueByDB(String key) {
        return key;
    }

}
