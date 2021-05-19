
import redis.clients.jedis.Jedis;

/**
 * @author:Li
 * @time: 2019/5/3 18:23
 * @version: 1.0.0
 * 测试redis数据结构
 */
public class RedisDataStructure {

    /**
     * jedis实例
     */
    private static Jedis jedis = JedisPoolTest.getJedis();

    public static void main(String[] args) {
        // testString();
        // testList();
        // testHash();
        // testSet();
        testZSet();
    }

    /**
     * 测试 string 格式的编码
     */
    private static void testString() {
        // key
        String key = "hello";

        jedis.set(key, "1");
        // int
        System.out.println(jedis.objectEncoding(key));

        jedis.set(key, "world");
        // embstr
        System.out.println(jedis.objectEncoding(key));

        jedis.set(key, "worldworldworldworldworldworldworldworldworld");
        // raw
        System.out.println(jedis.objectEncoding(key));
    }

    /**
     * 测试 list 格式的编码
     */
    private static void testList() {
        jedis.lpush("list", "a", "b");

        // quicklist
        System.out.println(jedis.objectEncoding("list"));
    }

    /**
     * 测试 hash 格式的编码
     */
    private static void testHash() {
        for (int i = 0; i < 100; i++) {
            jedis.hset("hash", "key" + String.valueOf(i), String.valueOf(i));
        }

        // ziplist
        System.out.println(jedis.objectEncoding("hash"));

        for (int i = 0; i < 513; i++) {
            jedis.hset("hash1", "key" + String.valueOf(i), String.valueOf(i));
        }

        // hashtable
        System.out.println(jedis.objectEncoding("hash1"));
    }

    /**
     * 测试 set 格式的编码
     */
    private static void testSet() {
        for (int i = 0; i < 10; i++) {
            jedis.sadd("set", String.valueOf(i));
        }
        // intset
        System.out.println(jedis.objectEncoding("set"));

        for (int i = 0; i < 10; i++) {
            jedis.sadd("set1", "s" + String.valueOf(i));
        }
        // hashtable
        System.out.println(jedis.objectEncoding("set1"));
    }

    /**
     * 测试 zset 格式的编码
     */
    private static void testZSet() {
        jedis.zadd("zset", 1, "a");
        // ziplist
        System.out.println(jedis.objectEncoding("zset"));

        jedis.zadd("zset1", 1, "aaaaaaaaaaaaaaaaaaaaaaaaa");
        // skiplist
        System.out.println(jedis.objectEncoding("zset1"));
    }


}
