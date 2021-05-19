
import redis.clients.jedis.Jedis;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

/**
 * @author:Li
 * @time: 2019/4/27 21:08
 * @version: 1.0.0
 */
public class RedisLock {

    /*
     *  原理:redis 的setnx如果key存在就会做任何操作  不存在就会set 根据这一点可以实现一个简单的锁
     *
     *  锁超时时不能直接del 如果在del之前有其他线程获得了锁, 那么可能造成锁的释放
     */

    /**
     * 锁超时过期时间  200方便看出测试效果
     */
    private static final long EXPIRED = 200;

    public static void main(String[] args) {
        // test1("lock:foo1");
        test2("lock:foo2");
        test2("lock:foo2");
    }

    /**
     * 测试1
     */
    private static void test1(String lock) throws InterruptedException {
        Runnable r = () -> {
            // 获取jedis实例
            Jedis jedis = JedisPoolTest.getJedis();
            // 尝试获取锁
            long lockValue = tryLock(jedis, lock);
            if (lockValue == 0) {
                System.out.println(Thread.currentThread().getName() + " #" + System.currentTimeMillis() + " 没抢到锁");
            } else {
                System.out.println(Thread.currentThread().getName() + " #" + System.currentTimeMillis() + " 抢到锁了 @" + lockValue);
                try {
                    // 休息释放锁
                    Thread.sleep(200);
                    unLock(jedis, lock, lockValue);
                    System.out.println(Thread.currentThread().getName() + "释放锁");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        };

        // 启动三个线程开始抢锁
        for (int i = 0; i < 3; i++) {
            new Thread(r).start();
        }

        Thread.sleep(200);

        // 再启动三个线程开始抢锁
        for (int i = 0; i < 3; i++) {
            new Thread(r).start();
        }
    }

    /**
     * 测试2
     */
    private static void test2(String lock) {
        CyclicBarrier c = new CyclicBarrier(3);
        for (int i = 0; i < 3; i++) {
            new Thread(new TestLock(c, lock)).start();
        }
    }

    static class TestLock implements Runnable {
        CyclicBarrier c;
        String lock;

        public TestLock(CyclicBarrier cyclicBarrier, String lock) {
            c = cyclicBarrier;
            this.lock = lock;
        }

        @Override
        public void run() {
            try {
                c.await();
                System.out.println(Thread.currentThread().getName() + " @" + System.currentTimeMillis() + " 开始抢锁 " + tryLock(JedisPoolTest.getJedis(), lock));
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (BrokenBarrierException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 尝试获取分布式锁
     *
     * @param jedis jedis连接
     * @param lock  锁名称
     * @return true获得到锁
     */
    public static long tryLock(Jedis jedis, String lock) {
        // 设置锁的过期时间
        Long lockValue = System.currentTimeMillis() + EXPIRED + 1;
        // 尝试获取锁
        Long setnx = jedis.setnx(lock, String.valueOf(lockValue));
        // 判断是否得到锁
        if (setnx == 1) {
            return lockValue;
        } else {
            // 获取锁的值
            Long oldLockValue = Long.valueOf(jedis.get(lock));

            // 判断锁是否超时
            if (oldLockValue < System.currentTimeMillis()) {
                // 将锁赋值 并获取为改变的值
                String getOldLockValue = jedis.getSet(lock, String.valueOf(lockValue));
                // 再进行判断
                if (Long.valueOf(getOldLockValue).equals(oldLockValue)) {
                    // 锁设置成功
                    return lockValue;
                } else {
                    // 其他线程抢到了锁
                    return 0;
                }
            } else {
                return 0;
            }
        }
    }

    /**
     * 释放分布式锁
     *
     * @param jedis   jedis连接
     * @param lock    锁名称
     * @param timeOut 超时时间
     */
    public static void unLock(Jedis jedis, String lock, long timeOut) {
        // 先获取锁
        String lockValue = jedis.get(lock);
        if (lockValue == null) {
            return;
        } else if (Long.valueOf(lockValue).equals(timeOut)) {
            // 删除key
            jedis.del(lock);
        }
    }

}
