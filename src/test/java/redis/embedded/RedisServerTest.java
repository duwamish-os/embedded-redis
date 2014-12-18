package redis.embedded;

import static com.google.common.base.Predicates.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.net.Socket;
import java.util.Arrays;
import java.util.Collection;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.embedded.RedisServer.LogLevel;

import com.google.common.base.Function;
import com.google.common.base.Predicate;

@RunWith(Parameterized.class)
public class RedisServerTest {
	
	@Parameters
    public static Collection<Object[]> data() {
        return Arrays.<Object[]> asList(new Object[][] { { true }, { false } });
    }

	private final boolean daemonize;
	private RedisServer redisServer;
	
	public RedisServerTest(boolean daemonize) {
		this.daemonize = daemonize;
	}

	@Before
	public void beforeMethod() throws IOException {
		// new File("/opt/redis-2.8.3/src/redis-server"), 
		System.out.println("*** Starting with daemonize " + daemonize);
		redisServer = new RedisServer(new File("/opt/redis-2.8.3/src/redis-server"), 6379).daemonize(daemonize).withLogLevel(LogLevel.VERBOSE);
	}

	@After
	public void afterMethod() {
		if(redisServer.isActive())
			redisServer.stop();
	}
	
	@Test(timeout = 11500L)
	public void testSimpleRun() throws Exception {
		System.out.println("*** Running with daemonize " + redisServer.isDaemonize());
		redisServer.start();
		Thread.sleep(1000L);
		redisServer.stop();
	}
	
	@Test(expected = RuntimeException.class)
	public void shouldNotAllowMultipleRunsWithoutStop() throws Exception {
		redisServer.start();
		redisServer.start();
	}
	
	@Test
	public void shouldAllowSubsequentRuns() throws Exception {
		redisServer.start();
		redisServer.stop();
		
		Thread.sleep(200L);
		redisServer.start();
		redisServer.stop();
		
		Thread.sleep(200L);
		redisServer.start();
		redisServer.stop();
	}
	
	@Test
	public void testSimpleOperationsAfterRun() throws Exception {
		redisServer.start();
		
		JedisPool pool = null;
		Jedis jedis = null;
		try {
			pool = new JedisPool("localhost", 6379);
			jedis = pool.getResource();
			jedis.mset("abc", "1", "def", "2");
			
			assertEquals("1", jedis.mget("abc").get(0));
			assertEquals("2", jedis.mget("def").get(0));
			assertEquals(null, jedis.mget("xyz").get(0));
			pool.returnResource(jedis);
		} finally {
			if (jedis != null)
				pool.returnResource(jedis);
		}
	}

    @Test
    public void shouldIndicateInactiveBeforeStart() throws Exception {
        assertFalse(redisServer.isActive());
    }

    @Test
    public void shouldIndicateActiveAfterStart() throws Exception {
        redisServer.start();
        assertTrue(redisServer.isActive());
    }

    @Test
    public void shouldIndicateInactiveAfterStop() throws Exception {
        redisServer.start();
        redisServer.stop();
        assertFalse(redisServer.isActive());
    }

	@Test
	public void socketShouldBeClosedAfterStop() throws Exception {
		redisServer.start();
		redisServer.stop();
		Socket s = null;
		try {
			s = new Socket("localhost", redisServer.getPort());
			fail("RedisServer socket still open.");
		} catch(IOException e) {
			// Expected
		} finally {
			if(s != null)
				s.close();
		}
	}
	
	@Test
	public void testMasterSlave() throws Exception {
		redisServer.withLogLevel(LogLevel.DEBUG).start();

		RedisServer slaveServer = new RedisServer(redisServer.getPort() + 1).slaveOf(redisServer);
		slaveServer.withLogLevel(LogLevel.DEBUG).start();

		Jedis master = new Jedis("localhost", redisServer.getPort());
		Jedis slave = new Jedis("localhost", slaveServer.getPort());
		try {
			master.set("foo", "bar");
			assertWaiting(1000, slave, get("foo"), equalTo("bar"));
		} finally {
			master.disconnect();
			slave.disconnect();
			slaveServer.stop();
		}
	}
	
	@Test(expected = JedisDataException.class)
	public void slaveShouldRejectWrite() throws Exception {
		redisServer.withLogLevel(LogLevel.DEBUG).start();

		RedisServer slaveServer = new RedisServer(redisServer.getPort() + 1).slaveOf(redisServer);
		slaveServer.withLogLevel(LogLevel.DEBUG).start();

		Jedis slave = new Jedis("localhost", slaveServer.getPort());
		try {
			slave.set("foo", "bar");
			fail("Write should be rejected.");
		} finally {
			slave.disconnect();
			slaveServer.stop();
		}
	}
	
	static Function<JedisCommands, String> get(final String key) {
		return new Function<JedisCommands, String>() {
			@Override
			public String apply(JedisCommands input) {
				return input.get(key);
			}
		};
	}

    public static <T,V> void assertWaiting(final long maxTimeToWait, final T obj, final Function<T, V> function, final Predicate<V> predicate) {
        final long start = System.currentTimeMillis();
        while( System.currentTimeMillis() < start + maxTimeToWait ) {
            final V result = function.apply(obj);
            if ( predicate.apply(result) ) {
                return;
            }
            try {
                Thread.sleep( 10 );
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
        throw new AssertionError("Expected not null, actual null.");
    }

}
