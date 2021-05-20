
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 * @author:Li
 * @time: 2019/4/27 19:05
 * @version: 1.0.0
 */
public class JedisPoolTest {

    /**
     * 创建连接池 需要 commons-pool2-2.6.0.jar
     */
    private static JedisPool pool = new JedisPool("127.0.0.1", 6379);

    /**
     * 获得jedis实例
     *
     * @return 实例
     */
    public static Jedis getJedis() {
        // 认证
        Jedis jedis = pool.getResource();
//        jedis.auth("admin123");
        return jedis;
    }


    public static void main(String[] args) {
        Jedis jedis = null;
        try {
            // 从连接池获取一个连接
            jedis = getJedis();
            // 执行命令
            jedis.set("a", "a");
            System.out.println(jedis.objectEncoding("a"));
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("遇到错误");
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

}
