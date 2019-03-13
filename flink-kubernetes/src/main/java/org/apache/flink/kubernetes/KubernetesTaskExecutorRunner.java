package org.apache.flink.kubernetes;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.security.SecurityConfiguration;
import org.apache.flink.runtime.security.SecurityUtils;
import org.apache.flink.runtime.taskexecutor.TaskManagerRunner;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.JvmShutdownSafeguard;
import org.apache.flink.runtime.util.SignalHandler;
import org.apache.flink.util.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.NotImplementedError;

import java.lang.reflect.UndeclaredThrowableException;
import java.util.Map;
import java.util.concurrent.Callable;


/**
 * This class is the executable entry point for running a TaskExecutor in a YARN container.
 */
public class KubernetesTaskExecutorRunner
{
	protected static final Logger LOG = LoggerFactory.getLogger(KubernetesTaskExecutorRunner.class);

	/** The process environment variables. */
	private static final Map<String, String> ENV = System.getenv();

	/** The exit code returned if the initialization of the yarn task executor runner failed. */
	private static final int INIT_ERROR_EXIT_CODE = 31;

	// ------------------------------------------------------------------------
	//  Program entry point
	// ------------------------------------------------------------------------

	/**
	 * The entry point for the YARN task executor runner.
	 *
	 * @param args The command line arguments.
	 */
	public static void main(String[] args) {
		EnvironmentInformation.logEnvironmentInfo(LOG, "YARN TaskExecutor runner", args);
		SignalHandler.register(LOG);
		JvmShutdownSafeguard.installAsShutdownHook(LOG);

		run(args);
	}

	/**
	 * The instance entry point for the YARN task executor. Obtains user group information and calls
	 * the main work method {@link TaskManagerRunner#runTaskManager(Configuration, ResourceID)}  as a
	 * privileged action.
	 *
	 * @param args The command line arguments.
	 */
	private static void run(String[] args) {
		try {
			LOG.debug("All environment variables: {}", ENV);

			Configuration configuration = getConfiguration(args);
			SecurityConfiguration sc = new SecurityConfiguration(configuration);

			final String containerId = getContainerId(configuration);

			// use the hostname passed by job manager
			final String taskExecutorHostname = getHostName();
			if (taskExecutorHostname != null) {
				configuration.setString(TaskManagerOptions.HOST, taskExecutorHostname);
			}

			SecurityUtils.install(sc);

			SecurityUtils.getInstalledContext().runSecured((Callable<Void>) () -> {
				TaskManagerRunner.runTaskManager(configuration, new ResourceID(containerId));
				return null;
			});
		}
		catch (Throwable t) {
			final Throwable strippedThrowable = ExceptionUtils.stripException(t, UndeclaredThrowableException.class);
			// make sure that everything whatever ends up in the log
			LOG.error("Kubernetes TaskManager initialization failed.", strippedThrowable);
			System.exit(INIT_ERROR_EXIT_CODE);
		}
	}

	private static String getHostName()
	{
		throw new NotImplementedError();
	}

	private static String getContainerId(Configuration configuration)
	{
		throw new NotImplementedError();
	}

	private static Configuration getConfiguration(String[] args)
	{
		throw new NotImplementedError();
	}

}
