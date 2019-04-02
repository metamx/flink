package org.apache.flink.kubernetes;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
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

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.Map;
import java.util.concurrent.Callable;


/**
 * This class is the executable entry point for running a TaskExecutor in a Kubernetes container.
 * It duplicates an entry point of {@link org.apache.flink.runtime.taskexecutor.TaskManagerRunner}
 */
public class KubernetesTaskManagerRunner {
	private static final Logger LOG = LoggerFactory.getLogger(KubernetesTaskManagerRunner.class);

	/**
	 * The process environment variables.
	 */
	private static final Map<String, String> ENV = System.getenv();

	/**
	 * The exit code returned if the initialization of the Kubernetes task executor runner failed.
	 */
	private static final int INIT_ERROR_EXIT_CODE = 31;

	// ------------------------------------------------------------------------
	//  Program entry point
	// ------------------------------------------------------------------------

	/**
	 * The entry point for the Kubernetes task executor runner.
	 *
	 * @param args The command line arguments.
	 */
	public static void main(String[] args) throws Exception {
		EnvironmentInformation.logEnvironmentInfo(LOG, "TaskManager", args);
		SignalHandler.register(LOG);
		JvmShutdownSafeguard.installAsShutdownHook(LOG);

		long maxOpenFileHandles = EnvironmentInformation.getOpenFileHandlesLimit();

		if (maxOpenFileHandles != -1L) {
			LOG.info("Maximum number of open file descriptors is {}.", maxOpenFileHandles);
		} else {
			LOG.info("Cannot determine the maximum number of open file descriptors");
		}

		final Configuration configuration = TaskManagerRunner.loadConfiguration(args);

		try {
			FileSystem.initialize(configuration);
		} catch (IOException e) {
			throw new IOException("Error while setting the default " +
				"filesystem scheme from configuration.", e);
		}

		SecurityUtils.install(new SecurityConfiguration(configuration));

		LOG.info("All environment variables: {}", ENV);

		try {

			SecurityUtils.getInstalledContext().runSecured((Callable<Void>) () -> {
				TaskManagerRunner.runTaskManager(configuration, new ResourceID(getContainerId()));
				return null;
			});
		} catch (Throwable t) {
			final Throwable strippedThrowable = ExceptionUtils.stripException(t, UndeclaredThrowableException.class);
			// make sure that everything whatever ends up in the log
			LOG.error("Kubernetes TaskManager initialization failed.", strippedThrowable);
			System.exit(INIT_ERROR_EXIT_CODE);
		}
	}

	private static String getContainerId() {
		if (ENV.containsKey(KubernetesResourceManager.FLINK_TM_RESOURCE_ID)) {
			return ENV.get(KubernetesResourceManager.FLINK_TM_RESOURCE_ID);
		} else {
			LOG.warn("ResourceID env variable {} is not found. Generating resource id",
				KubernetesResourceManager.FLINK_TM_RESOURCE_ID);
			return ResourceID.generate().getResourceIdString();
		}
	}

}
