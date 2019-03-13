package org.apache.flink.kubernetes.client;

import io.kubernetes.client.ApiClient;
import io.kubernetes.client.apis.CoreV1Api;
import io.kubernetes.client.util.Config;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import scala.NotImplementedError;

import java.io.IOException;
import java.util.Map;

public class KubernetesClientImpl implements KubernetesClient
{
	private final Configuration configuration;
	private final Map<String, String> environment;
	private final CoreV1Api coreV1Api;


	public KubernetesClientImpl(Configuration configuration, Map<String, String> environment) throws IOException
	{
		this.configuration = configuration;
		this.environment = environment;
		final ApiClient apiClient = Config.defaultClient();
		io.kubernetes.client.Configuration.setDefaultApiClient(apiClient);
		this.coreV1Api = new CoreV1Api(apiClient);
	}

	@Override
	public Endpoint createClusterService()
	{
		throw new NotImplementedError();
	}

	@Override
	public void createClusterPod(ResourceProfile resourceProfile)
	{
		throw new NotImplementedError();
	}

	@Override
	public void terminateClusterPod(ResourceID resourceID)
	{
		throw new NotImplementedError();
	}

	@Override
	public void stopAndCleanupCluster(String clusterId)
	{
		throw new NotImplementedError();
	}

	@Override
	public void logException(Exception e)
	{
		throw new NotImplementedError();
	}

	@Override
	public Endpoint getResetEndpoint(String flinkClusterId)
	{
		throw new NotImplementedError();
	}

	@Override
	public void close() throws Exception
	{
		throw new NotImplementedError();
	}
}
