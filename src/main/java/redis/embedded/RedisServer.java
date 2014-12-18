package redis.embedded;

import static java.io.File.createTempFile;
import static java.util.Arrays.asList;
import static org.apache.commons.io.FileUtils.deleteQuietly;
import static org.apache.commons.io.FileUtils.getTempDirectoryPath;
import static org.apache.commons.io.FileUtils.readFileToString;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.net.Socket;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;

import com.google.common.base.Strings;
import com.google.common.collect.Collections2;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.common.io.Resources;
import com.google.common.util.concurrent.Callables;
import com.google.common.util.concurrent.MoreExecutors;

public class RedisServer {
	
	private static enum RedisRunScriptEnum {
		WINDOWS_32("redis-server.exe"),
		WINDOWS_64("redis-server-64.exe"),
		UNIX("redis-server"),
		MACOSX("redis-server.app");
		
		private final String runScript;

		private RedisRunScriptEnum(String runScript) {
			this.runScript = runScript;
		}
		
		public static String getRedisRunScript() {
			String osName = System.getProperty("os.name");
			String osArch = System.getProperty("os.arch");
			
			if (osName.indexOf("win") >= 0) {
				if (osArch.indexOf("64") >= 0) {
					return WINDOWS_64.runScript;
				} else {
					return WINDOWS_32.runScript;
				}
			} else if (osName.indexOf("nix") >= 0 || osName.indexOf("nux") >= 0 || osName.indexOf("aix") > 0) {
				return UNIX.runScript;
			} else if ("Mac OS X".equals(osName)) {
				return MACOSX.runScript;
			} else {
				throw new RuntimeException("Unsupported os/architecture...: " + osName + " on " + osArch);
			}
		}
	}
	
	private static final String REDIS_READY_PATTERN = ".*The server is now ready to accept connections on port.*";
	private static final ExecutorService EXECUTOR_SERVICE = MoreExecutors.getExitingExecutorService((ThreadPoolExecutor) Executors.newCachedThreadPool());

	private final Logger logger = Logger.getLogger(getClass().getName());

	private final File command;
	private final Integer port;
	private LogLevel logLevel;
	private boolean daemonize;

	private volatile boolean active = false;
	private Process redisProcess;

	private RedisServer slaveOf;

	private Future<?> outputPrinter;
	private File pidFile;
	private File logFile;

	public RedisServer(File command, Integer port) {
		this.command = command;
		this.port = port;
	}

	public RedisServer(Integer port) throws IOException {
		this.port = port;
		this.command = extractExecutableFromJar(RedisRunScriptEnum.getRedisRunScript());
	}

	private File extractExecutableFromJar(String scriptName) throws IOException {
		File tmpDir = Files.createTempDir();
		tmpDir.deleteOnExit();

		File command = new File(tmpDir, scriptName);
		FileUtils.copyURLToFile(Resources.getResource(scriptName), command);
		command.deleteOnExit();
		command.setExecutable(true);
		
		return command;
	}
	
	public Integer getPort() {
		return port;
	}
	
	public RedisServer daemonize(boolean daemonize) {
		this.daemonize = daemonize;
		return this;
	}
	
	public boolean isDaemonize() {
		return daemonize;
	}
	
	public RedisServer slaveOf(RedisServer master) {
		this.slaveOf = master;
		return this;
	}

	public RedisServer withLogLevel(LogLevel logLevel) {
		this.logLevel = logLevel;
		return this;
	}
	
	public boolean isActive() {
		return active;
	}

	public synchronized void start() throws IOException {
		if (active) {
			throw new RuntimeException("This redis server instance is already running...");
		}

		redisProcess = createRedisProcessBuilder().start();
//		if(daemonize) {
//			redisProcess.destroy();
//		}
		
		BufferedReader redisOutput = awaitRedisServerReady();
		outputPrinter = EXECUTOR_SERVICE.submit(createRedisOutputPrinter(redisOutput));
		
		active = true;
	}

	private Runnable createRedisOutputPrinter(final BufferedReader redisOutput) {
		Runnable runnable = new Runnable() {
	        public void run() {
	            try {
					String line = null;
					while (!Thread.currentThread().isInterrupted()) {
						while((line = redisOutput.readLine()) != null)
							System.out.println("[" + port + "] " + line);
						Thread.sleep(10);
					}
				} catch (IOException e) {
					// Ignore IOE (typically "Stream closed") when we got interrupted
					if(!Thread.currentThread().isInterrupted()) {
						logger.warning("Could not read redis output: " + e);
						throw new RuntimeException(e);
					}
				} catch (InterruptedException e) {
					// Restore the interrupted status
					Thread.currentThread().interrupt();
				} finally {
					logger.info("[" + port + "] Closing redis output");
	            	IOUtils.closeQuietly(redisOutput);
				}
	        }
	    };
		return runnable;
	}

	private BufferedReader awaitRedisServerReady() throws IOException {
		if(daemonize) {
			waitUntil(5000, fileExists(new File(logFile.getAbsolutePath()), true));
			return awaitRedisServerReady(new FileInputStream(logFile));
		}
		return awaitRedisServerReady(redisProcess.getInputStream());
	}

	private BufferedReader awaitRedisServerReady(InputStream in)
			throws IOException {
		BufferedReader reader = new BufferedReader(new InputStreamReader(in));
		String outputLine = null;
		do {
			outputLine = reader.readLine();
		} while (outputLine != null && !outputLine.matches(getRedisReadyPattern()));
		return reader;
	}

	protected ProcessBuilder createRedisProcessBuilder() {
		final List<String> commands = Lists.newArrayList(command.getAbsolutePath());
		commands.addAll(getProcessBuilderCommandOptions());
		logger.info("Starting process `" + commands + "`");
		ProcessBuilder pb = new ProcessBuilder(commands);
		pb.directory(command.getParentFile());

		return pb;
	}
	
	public static void main(String[] args) throws IOException, InterruptedException {
		Process p = new ProcessBuilder("/opt/redis-2.8.3/src/redis-server", "--port", "6400", "--daemonize", "yes",
				"--pidfile", "/tmp/redis1.pid", "--logfile", "/tmp/redis1.log").start();
		Thread.sleep(2000);
		p.destroy();
		p.waitFor();
	}

	protected List<String> getProcessBuilderCommandOptions() {
		final List<String> commands = Lists.newArrayList("--port", Integer.toString(port));
		if(daemonize) {
			pidFile = new File(getTempDirectoryPath(), "redis-"+ port +".pid");
			logFile = new File(getTempDirectoryPath(), "redis-"+ port +".log");
			commands.addAll(asList(
					"--daemonize", "yes", "--pidfile", pidFile.getAbsolutePath(), "--logfile", logFile.getAbsolutePath()));
		}
		if(slaveOf != null) {
			commands.add("--slaveof");
			commands.add("localhost");
			commands.add(slaveOf.port.toString());
		}
		if(logLevel != null) {
			commands.add("--loglevel");
			commands.add(logLevel.name().toLowerCase());
		}
		return commands;
	}

	public synchronized void stop() {
		if (active) {
			outputPrinter.cancel(true);
			if(daemonize) {
				stopDaemon();
			}
			else {
				redisProcess.destroy();
				try {
					redisProcess.waitFor();
				} catch (InterruptedException e) {
					// Restore the interrupted status
					Thread.currentThread().interrupt();
				}
			}
			active = false;
		}
	}

	private void stopDaemon() {
		try {
			final String pid = readFileToString(pidFile);
			logger.info("Killing pid " + pid);
			// Doesn't support windows, this would have to be added if needed
			Runtime.getRuntime().exec("kill " + pid);
			
			waitUntil(1000, fileExists(pidFile, false), removeFiles(pidFile, logFile), kill(pid));
		} catch (IOException e) {
			logger.severe("Could not kill process for redis running on port " + port + " (pidfile " + pidFile + "): " + e);
			throw new RuntimeException(e);
		}
	}

	private Runnable removeFiles(final File ... files) {
		return new Runnable() {
			@Override
			public void run() {
				for (File file : files) {
					deleteQuietly(file);
				}
			}
		};
	}

	private Runnable kill(final String pid) {
		return new Runnable() {
			@Override
			public void run() {
		    	logger.info("Redis on port " + port + " (pid " + pid + ") did not stop, now killing -9 ...");
				try {
					Runtime.getRuntime().exec("kill -9 " + pid);
					deleteQuietly(pidFile);
					deleteQuietly(logFile);
				} catch (IOException e) {
					logger.severe("Could not kill -9 process for redis running on port " + port + " (pid "+ pid +", pidfile " + pidFile + "): " + e);
				}
			}
		};
	}
	
	protected String getRedisReadyPattern() {
		return REDIS_READY_PATTERN;
	}

	public static enum LogLevel {
		DEBUG, VERBOSE, NOTICE, WARNING		
	}
	
	static Callable<Boolean> fileExists(final File file, final boolean shouldExist) {
		return new Callable<Boolean>() {
			@Override
			public Boolean call() throws Exception {
				return shouldExist ? file.exists() : !file.exists();
			}
		};
	}

    static void waitUntil(final long maxTimeToWait, final Callable<Boolean> predicate) {
        waitUntil(maxTimeToWait, predicate, null, new Runnable() {
			@Override
			public void run() {
		        throw new IllegalStateException("Predicate not true within " + maxTimeToWait + " millis.");
			}});
    }

    static void waitUntil(final long maxTimeToWait, final Callable<Boolean> predicate, final Runnable success, Runnable failure) {
        final long start = System.currentTimeMillis();
        while( System.currentTimeMillis() < start + maxTimeToWait ) {
            try {
                if ( predicate.call() ) {
                	if(success != null)
                		success.run();
                    return;
                }
                Thread.sleep( 20 );
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            } catch (Exception e) {
            	throw new RuntimeException(e);
			}
        }
        if(failure != null)
        	failure.run();
    }
}
