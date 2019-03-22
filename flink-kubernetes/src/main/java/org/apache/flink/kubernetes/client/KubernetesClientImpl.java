package org.apache.flink.kubernetes.client;

import com.google.gson.JsonSyntaxException;
import io.kubernetes.client.ApiClient;
import io.kubernetes.client.ApiException;
import io.kubernetes.client.apis.CoreV1Api;
import io.kubernetes.client.models.V1Container;
import io.kubernetes.client.models.V1ContainerPort;
import io.kubernetes.client.models.V1EnvVar;
import io.kubernetes.client.models.V1ObjectMeta;
import io.kubernetes.client.models.V1Pod;
import io.kubernetes.client.models.V1PodSpec;
import io.kubernetes.client.util.Config;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.KubernetesResourceManager;
import org.apache.flink.kubernetes.client.exception.KubernetesClientException;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.NotImplementedError;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class KubernetesClientImpl implements KubernetesClient
{
	private static final Logger LOG = LoggerFactory.getLogger(KubernetesClient.class);

	private final Configuration configuration;
	private final Map<String, String> environment;
	private final CoreV1Api coreV1Api;
	private final String flinkTMImageName;
	private final String flinkNamespace;
	private final Map<String, ResourceProfile> podResourceProfiles = new HashMap<>();

	public KubernetesClientImpl(Configuration configuration, Map<String, String> environment) throws IOException
	{
		this.configuration = configuration;
		this.environment = environment;
		// Default user is used for managing deployments and their pods
		// Make sure default user has enough permissions for doing that
		final ApiClient apiClient = Config.defaultClient();
		io.kubernetes.client.Configuration.setDefaultApiClient(apiClient);
		this.coreV1Api = new CoreV1Api(apiClient);
		this.flinkNamespace = checkNotNull(this.environment.get(KubernetesResourceManager.FLINK_NAMESPACE));
		this.flinkTMImageName = checkNotNull(this.environment.get(KubernetesResourceManager.FLINK_TM_IMAGE));
	}

	@Override
	public Endpoint createClusterService()
	{
		throw new NotImplementedError();
	}

	@Override
	public void createClusterPod(ResourceProfile resourceProfile) throws KubernetesClientException
	{
		final ResourceID resourceID = ResourceID.generate();
		final String podName = getPodName(resourceID);

		LOG.info("Creating a cluster pod [{}] for a resource profile [{}]", podName, resourceProfile);
		V1Pod body = new V1Pod()
			.apiVersion("v1")
			.kind("Pod")
			.metadata(
				new V1ObjectMeta()
					.name(podName)
					.labels(new HashMap<String, String>()
							{{
								put("app", "flink");
								put("component", "taskmanager");
								put("role", "taskmanager");
								put("ResourceId", resourceID.getResourceIdString());
								put("CpuCores", String.valueOf(resourceProfile.getCpuCores()));
								put("MemoryInMB", String.valueOf(resourceProfile.getMemoryInMB()));
								put("HeapMemoryInMB", String.valueOf(resourceProfile.getHeapMemoryInMB()));
								put("DirectMemoryInMB", String.valueOf(resourceProfile.getDirectMemoryInMB()));
								put("NativeMemoryInMB", String.valueOf(resourceProfile.getNativeMemoryInMB()));
								put("NetworkMemoryInMB", String.valueOf(resourceProfile.getNetworkMemoryInMB()));
								put("OperatorsMemoryInMB", String.valueOf(resourceProfile.getOperatorsMemoryInMB()));
								resourceProfile.getExtendedResources().forEach(
									(key, resource) -> {
										put(key, resource.getName());
										put(resource.getName(), String.valueOf(resource.getValue()));
									}
								);
							}}
					))
			.spec(
				new V1PodSpec()
					.containers(Collections.singletonList(
						new V1Container()
							.name("taskmanager")
							.image(flinkTMImageName)
							.imagePullPolicy("IfNotPresent")
							.args(Collections.singletonList("taskmanager"))
							.ports(Arrays.asList(
								new V1ContainerPort().containerPort(6121).name("data"),
								new V1ContainerPort().containerPort(6122).name("rpc"),
								new V1ContainerPort().containerPort(6125).name("query")
							))
							.env(Arrays.asList(
								new V1EnvVar()
									.name("JOB_MANAGER_RPC_ADDRESS")
									.value("flink-jobmanager"),
								new V1EnvVar()
									.name(KubernetesResourceManager.FLINK_TM_RESOURCE_ID)
									.value(resourceID.getResourceIdString()),
								new V1EnvVar()
									.name(KubernetesResourceManager.FLINK_CLASS_TO_RUN)
									.value("org.apache.flink.kubernetes.KubernetesTaskManagerRunner")
							))
					)));
		try {
			coreV1Api.createNamespacedPod(flinkNamespace, body, false, null, null);
			podResourceProfiles.put(podName, resourceProfile);
		}
		catch (ApiException e) {
			final String message = String.format("Cannot create a pod for resource profile [%s]", resourceProfile);
			throw new KubernetesClientException(message, e);
		}
	}

	@Override
	public void terminateClusterPod(ResourceID resourceID) throws KubernetesClientException
	{
		final String podName = getPodName(resourceID);

		LOG.info("Terminating a cluster pod [{}] for a resource profile [{}]", podName, resourceID);
		try {
			coreV1Api.deleteNamespacedPod(podName, flinkNamespace, null, null, null, 0, null, null);
			podResourceProfiles.remove(podName);
		}
		catch (ApiException e) {
			if (e.getMessage().equals("Not Found")) {
				LOG.warn("Could not delete a pod [{}] as it was not found", podName);
			} else {
				final String message =
					String.format("Could not delete a pod [%s] for resource profile [%s]", podName, flinkNamespace);
				throw new KubernetesClientException(message, e);
			}
		}
		catch (JsonSyntaxException e) {
			// It's a known issue until the Swagger spec is updated to OpenAPI 3.0
			// https://github.com/kubernetes-client/java/issues/86
			// Simply ignoring the exception
		}
	}

	@Override
	public void stopAndCleanupCluster(String clusterId)
	{
		LOG.info("Stopping the cluster and deleting all its task manager pods");
		podResourceProfiles.forEach(
			(podName, resourceProfile) -> {
				try {
					coreV1Api.deleteNamespacedPod(podName, flinkNamespace, null, null, null, 0, null, null);
				}
				catch (ApiException e) {
					LOG.error("Could not delete a pod [{}]", podName, e);
				}
			}
		);
		podResourceProfiles.clear();
	}

	@Override
	public void logException(Exception e)
	{
		LOG.error("Exception occurred", e);
	}

	@Override
	public Endpoint getResetEndpoint(String flinkClusterId)
	{
		throw new NotImplementedError();
	}

	@Override
	public void close()
	{
		throw new NotImplementedError();
	}

	private String getPodName(ResourceID resourceId)
	{
		return "flink-taskmanager-" + resourceId.getResourceIdString();
	}
}
