package redis.embedded;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;

import com.google.common.collect.Lists;

public class RedisSentinel extends RedisServer {

	private final RedisServer master;
	private final RedisServer[] slaves;
	
	private int masterDownAfterMilliseconds = 1000;
	private File configFile;

	public RedisSentinel(File command, Integer port, RedisServer master, RedisServer ... slaves) throws IOException {
		super(command, port);
		this.master = master;
		this.slaves = slaves;
	}

	public RedisSentinel(Integer port, RedisServer master, RedisServer ... slaves) throws IOException {
		super(port);
		this.master = master;
		this.slaves = slaves;
	}
	
	public RedisSentinel masterDownAfterMilliseconds(int millis) {
		masterDownAfterMilliseconds = millis;
		return this;
	}
	
	@Override
	protected ProcessBuilder createRedisProcessBuilder() {
		this.configFile = createConfigFile();
		return super.createRedisProcessBuilder();
	}
	
	@Override
	protected List<String> getProcessBuilderCommandOptions() {
		List<String> result = Lists.newArrayList(configFile.getAbsolutePath(), "--sentinel");
		result.addAll(super.getProcessBuilderCommandOptions());
		return result;
	}

	private File createConfigFile() {
		
		final StringBuilder sb = new StringBuilder();
		sb.append("sentinel monitor mymaster 127.0.0.1 ").append(master.getPort()).append(" 1");
		sb.append("\nsentinel down-after-milliseconds mymaster ").append(masterDownAfterMilliseconds);
		sb.append("\nsentinel failover-timeout mymaster 900000");
		sb.append("\nsentinel parallel-syncs mymaster 1");
		for (RedisServer slave : slaves) {
			sb.append("\nsentinel known-slave mymaster 127.0.0.1 ").append(slave.getPort());
		}
		
		try {
			System.out.println("Writing sentinel conf:\n" + sb.toString());
			final File configFile = File.createTempFile("sentinel-", ".conf");
			FileUtils.write(configFile, sb.toString());
			return configFile;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	@Override
	protected String getRedisReadyPattern() {
		return ".*Sentinel runid is.*";
	}

}
