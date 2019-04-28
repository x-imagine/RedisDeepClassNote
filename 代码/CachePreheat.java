package com.edit.redis;

import redis.clients.jedis.Jedis;

/**
 * @author:Li
 * @time: 2019/4/28 20:16
 * @version: 1.0.0
 * 缓存预热决解决方案
 */
public class CachePreheat {

    public static void main(String[] args) throws InterruptedException {
        // 缓存预热是为了防止第一次缓存未命中
        // 一般用于活动开始
        // 如果数据量不大 可以直接运行时添加

        // 从连接池获取jedis
        Jedis jedis = JedisPoolTest.getJedis();
        exam1(jedis, "hot:exam1:k1", "hot:exam1:k2", "hot:exam1:k3", "hot:exam1:k4", "hot:exam1:k5");
        exam2(jedis, "hot:exam2:k1", "hot:exam2:k2", "hot:exam2:k3", "hot:exam2:k4", "hot:exam2:k5");
    }

    /**
     * 直接导入
     */
    private static void exam1(Jedis jedis, String... keys) {
        // 遍历key 添加到redis
        for (String key : keys) {
            // 添加到缓存
            jedis.set(key, getValueByDB(key));
            jedis.expire(key, 30);
            System.out.println("set cache " + key);
        }
    }

    /**
     * 定时刷新缓存
     */
    private static void exam2(Jedis jedis, String... keys) throws InterruptedException {
        // 模拟quartz 定时器 三秒执行 刷新一次缓存
        for (; ; ) {
            Thread.sleep(3000);
            // 遍历key 添加到redis
            for (String key : keys) {
                // 添加到缓存
                jedis.set(key, getValueByDB(key));
                jedis.expire(key, 30);
                System.out.println("set cache " + key);
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
