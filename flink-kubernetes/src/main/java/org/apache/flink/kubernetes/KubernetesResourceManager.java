package org.apache.flink.kubernetes;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.client.KubernetesClient;
import org.apache.flink.kubernetes.client.KubernetesClientImpl;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.groups.JobManagerMetricGroup;
import org.apache.flink.runtime.resourcemanager.JobLeaderIdService;
import org.apache.flink.runtime.resourcemanager.ResourceManager;
import org.apache.flink.runtime.resourcemanager.exceptions.ResourceManagerException;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManager;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

public class KubernetesResourceManager extends ResourceManager<ResourceID>
{

	protected static final Logger LOG = LoggerFactory.getLogger(KubernetesResourceManager.class);

	private final Configuration configuration;
	private final Map<String, String> environment;

	/** Client to communicate with the Node manager and launch TaskExecutor processes. */
	private KubernetesClient nodeManagerClient;

	public KubernetesResourceManager(
		RpcService rpcService,
		String resourceManagerEndpointId,
		ResourceID resourceId,
		Configuration flinkConfig,
		Map<String, String> env,
		HighAvailabilityServices highAvailabilityServices,
		HeartbeatServices heartbeatServices,
		SlotManager slotManager,
		MetricRegistry metricRegistry,
		JobLeaderIdService jobLeaderIdService,
		ClusterInformation clusterInformation,
		FatalErrorHandler fatalErrorHandler,
		@Nullable String webInterfaceUrl,
		JobManagerMetricGroup jobManagerMetricGroup
	)
	{
		super(
			rpcService,
			resourceManagerEndpointId,
			resourceId,
			highAvailabilityServices,
			heartbeatServices,
			slotManager,
			metricRegistry,
			jobLeaderIdService,
			clusterInformation,
			fatalErrorHandler,
			jobManagerMetricGroup
		);
		this.configuration = flinkConfig;
		this.environment = env;
	}

	@Override
	protected void initialize() throws ResourceManagerException
	{
		try{
			nodeManagerClient = new KubernetesClientImpl(configuration, environment);
		} catch (IOException e) {
			throw new ResourceManagerException("Error while initializing K8s client", e);
		}
	}

	@Override
	protected void internalDeregisterApplication(
		ApplicationStatus finalStatus, @Nullable String optionalDiagnostics
	) throws ResourceManagerException
	{
		LOG.info("Shutting down and cleaning the cluster up.");
		nodeManagerClient.stopAndCleanupCluster(null);
	}

	@Override
	public Collection<ResourceProfile> startNewWorker(ResourceProfile resourceProfile)
	{
		LOG.info("Starting a new worker.");
		nodeManagerClient.createClusterPod(resourceProfile);
		return Collections.singletonList(resourceProfile);
	}

	@Override
	public boolean stopWorker(ResourceID worker)
	{
		LOG.info("Stopping worker {}.", worker.getResourceID());
		try {
			nodeManagerClient.terminateClusterPod(worker);
			return true;
		} catch (Exception e) {
			LOG.error("Could not terminate a worker", e);
			return false;
		}
	}

	@Override
	protected ResourceID workerStarted(ResourceID resourceID)
	{
		// TODO Hooray it started. Remove from pending
		return resourceID;
	}
}
