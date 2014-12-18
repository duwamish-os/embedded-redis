package redis.embedded;

import static com.google.common.base.Predicates.containsPattern;
import static com.google.common.base.Predicates.equalTo;
import static com.google.common.collect.Iterables.find;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static redis.embedded.RedisServerTest.assertWaiting;
import static redis.embedded.RedisServerTest.get;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.Socket;
import java.util.Arrays;
import java.util.concurrent.Callable;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Protocol;
import redis.embedded.RedisServer.LogLevel;

public class RedisSentinelTest {
	
	public static void main(String[] args) throws IOException, InterruptedException {

		RedisServer redisServer = new RedisServer(new File("/home/magro/proj/redis/src/redis-server"), 6379).withLogLevel(LogLevel.DEBUG);
		RedisServer slaveServer = new RedisServer(new File("/home/magro/proj/redis/src/redis-server"), redisServer.getPort() + 1).slaveOf(redisServer);
		RedisSentinel sentinel = new RedisSentinel(new File("/home/magro/proj/redis/src/redis-server"), 26379, redisServer, slaveServer);

		sentinel.masterDownAfterMilliseconds(1000).daemonize(true).withLogLevel(LogLevel.NOTICE).start();
		
		Thread.sleep(Long.MAX_VALUE);
	}

	private RedisServer redisServer;
	private RedisServer slaveServer;
	private RedisSentinel sentinel;
	
	private static final String REDIS_HOME = "/opt/redis-2.8.3"; // "/home/magro/proj/redis"

	@Before
	public void beforeMethod() throws IOException {
		// new File("/opt/redis-2.8.3/src/redis-server"), 
		redisServer = new RedisServer(new File(REDIS_HOME + "/src/redis-server"), 6379).withLogLevel(LogLevel.DEBUG);
		slaveServer = new RedisServer(new File(REDIS_HOME + "/src/redis-server"), redisServer.getPort() + 1).slaveOf(redisServer);
		sentinel = new RedisSentinel(new File(REDIS_HOME + "/src/redis-server"), 26379, redisServer, slaveServer);
	}

	@After
	public void afterMethod() {
		if(sentinel.isActive())
			sentinel.stop();
		if(slaveServer.isActive())
			slaveServer.stop();
		if(redisServer.isActive())
			redisServer.stop();
	}

    @Test(timeout = 1000)
    public void shouldIndicateActiveAfterStart() throws Exception {
    	sentinel.start();
        assertTrue(sentinel.isActive());
    }

	@Test
	public void socketShouldBeClosedAfterStop() throws Exception {
		sentinel.start();
		sentinel.stop();
		Socket s = null;
		try {
			s = new Socket("localhost", sentinel.getPort());
			fail("Sentinel socket still open.");
		} catch(IOException e) {
			// Expected
		} finally {
			if(s != null)
				s.close();
		}
	}
	
	@Test
	public void testMasterSlave() throws Exception {
		redisServer.daemonize(true).withLogLevel(LogLevel.DEBUG).start();
		Thread.sleep(1000);
		slaveServer.daemonize(true).withLogLevel(LogLevel.DEBUG).start();

//		System.out.println("**** for slave ...");
//		assertWaiting(60000, socketOpen(slaveServer.getPort()));

		System.out.println("**** starting sentinel ...");
		Thread.sleep(10000);
//		sentinel.masterDownAfterMilliseconds(1000).daemonize(false).withLogLevel(LogLevel.NOTICE).start();

		Jedis master = new Jedis("127.0.0.1", redisServer.getPort());
		Jedis slave = new Jedis("127.0.0.1", slaveServer.getPort());
		try {
			master.set("foo", "bar");
			RedisServerTest.assertWaiting(5000, slave, get("foo"), equalTo("bar"));
			System.out.println("**** WAITING before stopping master...");
			Thread.sleep(5000);

//			System.out.println("*** disconnecting master client");
			master.disconnect();
			System.out.println("*** stopping master");
			redisServer.stop();
			
//			Thread.sleep(1500);
//			String role = Iterables.find(Arrays.asList(slave.info().split("\n")), Predicates.containsPattern("role:"));
//			System.out.println("*** Role: " + role);
			
//			System.out.println("**** WAITING...");
//			Thread.sleep(5000);

//			System.out.println("*** checking slave is slave ");
//			assertWaiting(1000, slave, infoRole(), equalTo("role:slave"));
			System.out.println("*** checking slave becomes master");
			RedisServerTest.assertWaiting(10000, slave, infoRole(), equalTo("role:master"));
			String role = Iterables.find(Arrays.asList(slave.info().split("\n")), Predicates.containsPattern("role:"));
			System.out.println("*** Role: " + role);
			slave.set("foo", "baz");
//			
			redisServer.start();
			master = new Jedis("localhost", redisServer.getPort());
			RedisServerTest.assertWaiting(5000, master, get("foo"), equalTo("baz"));
			
		} finally {
//			master.disconnect();
			slave.disconnect();
		}
	}
	
	static Function<Jedis, String> infoRole() {
		return new Function<Jedis, String>() {
			@Override
			public String apply(Jedis input) {
				String info = input.info();
				// System.out.println("Got info \n" + info);
				return find(Arrays.asList(info.split("\n")), containsPattern("role:"));
			}
		};
	}
	
	static Callable<Boolean> socketOpen(final int port) {
		return new Callable<Boolean>() {
			@Override
			public Boolean call() throws Exception {
				Socket s = null;
				try {
					s = new Socket("localhost", port);
					return true;
				} catch(IOException e) {
					return false;
				} finally {
					if(s != null)
						s.close();
				}
			}
		};
	}

    public static <T,V> void assertWaiting(final long maxTimeToWait, final Callable<Boolean> predicate) throws Exception {
        final long start = System.currentTimeMillis();
        while( System.currentTimeMillis() < start + maxTimeToWait ) {
            if ( predicate.call() ) {
                Thread.sleep( 100 );
                return;
            }
            try {
                Thread.sleep( 50 );
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
        throw new AssertionError("Expected not null, actual null.");
    }

}
