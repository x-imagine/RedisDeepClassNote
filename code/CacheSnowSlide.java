package com.edit.redis;

import redis.clients.jedis.Jedis;

import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author:Li
 * @time: 2019/4/28 19:05
 * @version: 1.0.0
 * redis雪崩处理方案
 */
public class CacheSnowSlide {

    public static void main(String[] args) {
        // 从连接池获取链接
        Jedis jedis = JedisPoolTest.getJedis();

        // for (int i = 0; i < 5; i++) {
        //     exam1(jedis, "snow:" + i, String.valueOf(i), 60);
        // }

        // for (int i = 0; i < 5; i++) {
        //     new Thread(() -> {
        //         try {
        //             exam2(JedisPoolTest.getJedis(), "snow:exam2:1");
        //         } catch (InterruptedException e) {
        //             e.printStackTrace();
        //         }
        //     }).start();
        // }

        // 启动监听MQ
        new Thread(() -> {
            try {
                execMQ();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }).start();

        for (int i = 0; i < 5; i++) {
            new Thread(() -> {
                try {
                    exam3(JedisPoolTest.getJedis(), "snow:exam3:" + new Random().nextInt(2));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }).start();
        }
    }

    /**
     * 随机时间  添加时策略
     * 可以给key过期时间随机增加 原理: 雪崩效应就是大量的key同时过期, 导致并发全到数据库, 造成压力过大
     * <p>
     * 优点: 简单方便
     * 缺点: 时间不宜维护
     */
    private static void exam1(Jedis jedis, String key, String value, int seconds) {
        // 设置值
        jedis.set(key, value);
        // 设置过期时间 随机多分配几分钟这样可以防止同时失效
        jedis.expire(key, seconds + (new Random().nextInt(5) * 60));
        // 打印过期时间
        System.out.println("ttl: #" + jedis.ttl(key));
    }

    /**
     * 互斥锁 同缓存击穿策略 查找时策略
     * <p>
     * 优点: 思路简单 保证数据一致性
     * 缺点: 代码复杂度增大  存在死锁概率
     */
    private static String exam2(Jedis jedis, String key) throws InterruptedException {
        // 获取数据
        String s = jedis.get(key);
        if (s == null) {
            // 获得锁
            long lockValue = RedisLock.tryLock(jedis, "lock:snow1");
            if (lockValue != 0) {
                System.out.println("by$ BD");
                // 从数据库查找
                String valueByDB = getValueByDB(key);
                // 插入到redis
                jedis.set(key, valueByDB);
                // 释放锁
                RedisLock.unLock(jedis, "lock:snow1", lockValue);
                return valueByDB;
            } else {
                // 没获得到锁 休眠  已经有其他线程在查询数据了
                Thread.sleep(200);
                return exam2(jedis, key);
            }
        } else {
            System.out.println("by$ Cache");
            return s;
        }
    }

    /**
     * 创建阻塞队列
     */
    private static LinkedBlockingQueue<String> queue = new LinkedBlockingQueue<>(200);

    /**
     * 阻塞队列的方式  查找时策略
     * 优点: 思路简单
     * 缺点: 阻塞队列 代码复杂度增大
     */
    private static String exam3(Jedis jedis, String key) throws InterruptedException {
        // 获取值
        String value = jedis.get(key);
        // 如果值过期
        if (value == null) {
            queue.put(key);
            Thread.sleep(50);
            return exam3(jedis, key);
        } else {
            System.out.println("by Cache");
            return value;
        }
    }

    /**
     * 监听处理MQ
     */
    private static void execMQ() throws InterruptedException {
        // 从连接池获取jedis实例
        Jedis jedis = JedisPoolTest.getJedis();
        // 监听队列
        for (; ; ) {
            // 获得队列的key
            String key = queue.take();
            // 查看redis是否有数据
            String value = jedis.get(key);
            // 如果有数据证明队列头已经处理完毕
            if (value == null) {
                // 没有数据查询数据库
                jedis.set(key, getValueByDB(key));
                jedis.expire(key, 20);
                System.out.println("queue DB");
            } else {
                System.out.println("queue Cache");
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
