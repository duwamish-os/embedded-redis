package redis.embedded;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.Socket;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.JedisPool;

public class RedisServerTest {

	private RedisServer redisServer;

	@Before
	public void beforeMethod() throws IOException {
		redisServer = new RedisServer(6379);
	}

	@After
	public void afterMethod() {
		if(redisServer.isActive())
			redisServer.stop();
	}
	
	@Test(timeout = 1500L)
	public void testSimpleRun() throws Exception {
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
		redisServer.start();

		RedisServer slaveServer = new RedisServer(redisServer.getPort() + 1).slaveOf(redisServer);
		slaveServer.start();

		Jedis master = new Jedis("localhost", redisServer.getPort());
		Jedis slave = new Jedis("localhost", slaveServer.getPort());
		try {

			master.set("foo", "bar");
			 assertEquals("bar", master.get("foo"));

			Thread.sleep(3000);
			 assertEquals("bar", slave.get("foo"));
			
			// RedisServerTest.<JedisCommands, String> assertWaitingWithProxy(Predicates.equalTo("bar"), 1000, slave).get("foo");
			
			
		} finally {
			master.disconnect();
			slave.disconnect();
			slaveServer.stop();
		}
	}


    @java.lang.SuppressWarnings("unchecked")
    public static <T,V> T assertWaitingWithProxy(final Predicate<V> predicate, final long maxTimeToWait, final T objectToProxy) {
        final Class<?>[] interfaces = objectToProxy.getClass().getInterfaces();
        return (T) Proxy.newProxyInstance( Thread.currentThread().getContextClassLoader(),
                interfaces,
                new InvocationHandler() {
                    @Override
                    public Object invoke(final Object proxy, final Method method, final Object[] args) throws Throwable {
                        return assertPredicateWaiting(predicate, maxTimeToWait, objectToProxy, method, args);
                    }
                } );
    }

    private static <V> V assertPredicateWaiting(final Predicate<V> predicate, final long maxTimeToWait, final Object obj, final Method method, final Object[] args) throws Exception {
        final long start = System.currentTimeMillis();
        while( System.currentTimeMillis() < start + maxTimeToWait ) {
            @java.lang.SuppressWarnings("unchecked")
            final V result = (V) method.invoke(obj, args);
            if ( predicate.apply(result) ) {
                return result;
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
