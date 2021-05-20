
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import java.nio.charset.Charset;
import java.util.List;

/**
 * @author:Li
 * @time: 2019/4/28 16:06
 * @version: 1.0.0
 * 缓存穿透解决方案
 */
public class CacheThrough {

    public static void main(String[] args) {
        // 从连接池获取jedis实例
        Jedis jedis = JedisPoolTest.getJedis();

        // 初始化bloom
        bloomInit(jedis);

        exam1(jedis, "a");
        exam1(jedis, "b");
        exam1(jedis, "c");
        exam1(jedis, "d");
        exam1(jedis, "7");
        System.out.println();

        for (int i = 0; i < 5; i++) {
            exam2(jedis);
        }
    }

    /**
     * 创建bloom过滤器
     */
    private static BloomFilter<String> filter = BloomFilter.create(Funnels.stringFunnel(Charset.defaultCharset()), 2000);

    /**
     * 初始化bloom过滤器
     */
    private static void bloomInit(Jedis jedis) {
        // 遍历所有key将所有的数据添加bloom
        ScanParams scanParams = new ScanParams().count(100);
        // 指针
        String cursor = "0";
        do {
            // 遍历
            ScanResult<String> scan = jedis.scan(cursor, scanParams);
            // 获得所有key
            List<String> result = scan.getResult();
            result.forEach(s -> filter.put(s));
            cursor = scan.getStringCursor();
        } while (!"0".equals(cursor));
        System.out.println("boolmFilterCount#: " + filter.approximateElementCount());
    }

    /**
     * 布隆过滤器的巨大用处就是, 能够迅速判断一个元素是否在一个集合中
     * 1. 网页爬虫对URL的去重, 避免爬取相同的URL地址
     * 2. 反垃圾邮件, 从数十亿个垃圾邮件列表中判断某邮箱是否垃圾邮箱（同理, 垃圾短信）
     * 3. 缓存击穿, 将已存在的缓存放到布隆过滤器中, 当黑客访问不存在的缓存时迅速返回避免缓存及DB挂掉
     * <p>
     * 优点: 思路简单, 保证一致性, 性能强
     * 缺点: 代码复杂度大, 需要维护一个集合来放缓存的key, bloom不支持删操作
     */
    private static String exam1(Jedis jedis, String key) {
        String value = jedis.get(key);
        if (value == null) {
            // 如果不存在查看bloom是否存在
            if (!filter.mightContain(key)) {
                System.out.println("none");
                return "";
            } else {
                // 查询数据库
                String valueByDB = getValueByDB(key, false);
                System.out.println("by DB");
                jedis.set(key, valueByDB);
                return valueByDB;
            }
        } else {
            System.out.println("by Cache");
            return value;
        }
    }

    /**
     * 设置空值, 如果查询数据库, 没有查到的话设置一个会过期的空值
     * <p>
     * 优点: 思路简单
     * 缺点: 需要维护一个新的key
     */
    private static String exam2(Jedis jedis) {
        // 查询缓存
        String value = jedis.get("null:key1");
        // 缓存没有查询数据库
        if (value == null) {
            // 查询数据库
            String valueByDB = getValueByDB("null:key1");
            System.out.println("by DB");
            // 如果数据库为空
            if (valueByDB == null) {
                valueByDB = "";
            }
            // set一个空值
            jedis.set("null:key1", valueByDB);
            jedis.expire("null:key1", 60 * 2);
            return "";
        } else {
            System.out.println("by Cache");
            return value;
        }
    }


    /**
     * 模拟数据库查询
     */
    private static String getValueByDB(String key) {
        return getValueByDB(key, true);
    }

    /**
     * 模拟查询数据库
     */
    private static String getValueByDB(String key, boolean none) {
        if (none) {
            return null;
        } else {
            return key;
        }
    }

}
