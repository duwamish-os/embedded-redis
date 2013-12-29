package redis.embedded;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import redis.clients.jedis.Jedis;
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
}
