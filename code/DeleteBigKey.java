
import redis.clients.jedis.Jedis;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.Tuple;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author:Li
 * @time: 2019/4/27 19:16
 * @version: 1.0.0
 * 不可以直接删除 除string外其他类型的值, 如果很大, 可能会造成长时间阻塞, 造成雪崩效应, 推荐使用scan的方式进行删除
 */
public class DeleteBigKey {

    public static void main(String[] args) {
        // 从连接池获取一个jedis连接
        Jedis jedis = JedisPoolTest.getJedis();

        // 记录开始时间
        long start = System.currentTimeMillis();

        // 创建 biglist
        // createBigList(jedis);
        // 删除 biglist
        // deleteBigList(jedis);

        // 创建bighash
         createBigHash(jedis);
        // 删除bighash
         deleteBigHash(jedis);

        // 创建bigset
        // createBigSet(jedis);
        // 删除bigset
        // deleteBigSet(jedis);

        // 创建bigzset
        //createBigZset(jedis);
        // 删除bigzset
        //deleteBigZset(jedis);

        // 记录结束时间
        long end = System.currentTimeMillis();
        // 关闭连接
        jedis.close();
        System.out.println("use TimeMillis: " + (end - start) + "ms");
    }

    /**
     * 创建biglist
     *
     * @param jedis 连接
     */
    private static void createBigList(Jedis jedis) {
        // 创建一个数组
        String[] strings = new String[2000];
        // 将数组赋值
        for (int i = 0; i < strings.length; i++) {
            strings[i] = String.valueOf(i + 1);
        }
        // push数据
        Long len = jedis.lpush("biglist", strings);
        // redis 的 list 的总量
        System.out.println("len: " + len);
    }

    /**
     * 删除biglist  使用ltrim进行渐进删除
     *
     * @param jedis 连接
     */
    private static void deleteBigList(Jedis jedis) {
        String key = "biglist";

        // 获得list长度
        long len = jedis.llen(key);
        // 计数器
        int index = 0;
        // 每次删除多少个
        int left = 100;
        while (index < len) {
            // 每次从左侧截掉100个  保留left-len区间的元素
            jedis.ltrim(key, left, len);
            // 计数器增加
            index += left;
        }
        // 最终删除key
        jedis.del(key);
        System.out.println("删除完毕");
    }

    /**
     * 创建bighash
     *
     * @param jedis 连接
     */
    private static void createBigHash(Jedis jedis) {
        // 创建map
        Map<String, String> map = new HashMap<>();

        // 循环插值
        for (int i = 0; i < 2000; i++) {
            map.put(String.valueOf(i + 1), String.valueOf(i + 1));
        }

        // 批量添加
        jedis.hmset("bighash", map);
        System.out.println("生成成功");
    }

    /**
     * 删除bighash  使用hsacn＋hdel
     *
     * @param jedis 连接
     */
    private static void deleteBigHash(Jedis jedis) {
        String key = "bighash";

        // 遍历100条
        ScanParams scan = new ScanParams().count(100);
        // 指针
        String cursor = "0";
        do {
            // 进行scan遍历
            ScanResult<Map.Entry<String, String>> hscan = jedis.hscan(key, cursor, scan);
            // 得到所有的值
            List<Map.Entry<String, String>> result = hscan.getResult();
            // 遍历删除
            result.forEach(e -> jedis.hdel(key, e.getKey()));
            // 获取到遍历完毕的指针
            cursor = hscan.getStringCursor();
            System.out.println("cursor: " + cursor);
        } while (!"0".equals(cursor));
        // 最终删除key
        jedis.del(key);
    }

    /**
     * 创建bigset
     *
     * @param jedis 连接
     */
    private static void createBigSet(Jedis jedis) {
        // 插入数据
        for (int i = 0; i < 2000; i++) {
            jedis.sadd("bigset", String.valueOf(i + 1));
        }
        System.out.println("size: " + jedis.scard("bigset"));
    }

    /**
     * 删除bigset  使用sscan+srem渐进式删除
     *
     * @param jedis 连接
     */
    private static void deleteBigSet(Jedis jedis) {
        String key = "bigset";

        // 遍历100条
        ScanParams scan = new ScanParams().count(100);
        // 指针
        String cursor = "0";
        do {
            // 进行scan遍历
            ScanResult<String> sscan = jedis.sscan(key, cursor, scan);
            // 结果集对象
            List<String> result = sscan.getResult();
            // 遍历删除
            result.forEach(e -> jedis.srem(key, e));
            // 获取到遍历完毕的指针
            cursor = sscan.getStringCursor();
            System.out.println("cursor: " + cursor);
        } while (!"0".equals(cursor));
        // 最终删除key
        jedis.del(key);
    }

    /**
     * 创建bigzset
     *
     * @param jedis 连接
     */
    private static void createBigZset(Jedis jedis) {
        // 遍历添加数据
        for (int i = 0; i < 2000; i++) {
            jedis.zadd("bigzset", i + 1, String.valueOf(i + 1));
        }
        System.out.println(jedis.zcard("bigzset"));
    }

    /**
     * 删除bigzset zscan+ztem
     *
     * @param jedis 连接
     */
    private static void deleteBigZset(Jedis jedis) {
        String key = "bigzset";

        // 遍历100条
        ScanParams scan = new ScanParams().count(100);
        // 指针
        String cursor = "0";
        do {
            // 进行scan遍历
            ScanResult<Tuple> zscan = jedis.zscan(key, cursor, scan);
            // 结果集对象
            List<Tuple> result = zscan.getResult();
            // 遍历删除
            result.forEach(e -> jedis.zrem(key, e.getElement()));
            // 获取到遍历完毕的指针
            cursor = zscan.getStringCursor();
            System.out.println("cursor: " + cursor);
        } while (!"0".equals(cursor));
        // 最终删除key
        jedis.del(key);
    }

}
