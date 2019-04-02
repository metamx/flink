package org.apache.flink.kubernetes.client;

import org.apache.flink.kubernetes.KubernetesResourceManager;
import org.apache.flink.kubernetes.client.exception.KubernetesClientException;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;

import com.google.gson.JsonSyntaxException;
import io.kubernetes.client.ApiClient;
import io.kubernetes.client.ApiException;
import io.kubernetes.client.apis.CoreV1Api;
import io.kubernetes.client.models.V1Container;
import io.kubernetes.client.models.V1ContainerPort;
import io.kubernetes.client.models.V1DeleteOptions;
import io.kubernetes.client.models.V1EnvVar;
import io.kubernetes.client.models.V1ObjectMeta;
import io.kubernetes.client.models.V1Pod;
import io.kubernetes.client.models.V1PodSpec;
import io.kubernetes.client.util.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import scala.NotImplementedError;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Kubernetes Client.
 * It uses default service client to operate with kubernetes abstractions.
 */
public class DefaultKubernetesClient implements KubernetesClient {
	private static final Logger LOG = LoggerFactory.getLogger(KubernetesClient.class);

	private final CoreV1Api coreV1Api;
	private final String flinkTMImageName;
	private final String flinkNamespace;
	private final Map<String, ResourceProfile> podResourceProfiles = new HashMap<>();

	public DefaultKubernetesClient(Map<String, String> environment) throws IOException {
		//TODO: add a property to specify a user
		// Default user is used for managing deployments and their pods
		// Make sure default user has enough permissions for doing that
		final ApiClient apiClient = Config.defaultClient();
		io.kubernetes.client.Configuration.setDefaultApiClient(apiClient);
		this.coreV1Api = new CoreV1Api(apiClient);
		this.flinkNamespace = checkNotNull(environment.get(KubernetesResourceManager.FLINK_NAMESPACE));
		this.flinkTMImageName = checkNotNull(environment.get(KubernetesResourceManager.FLINK_TM_IMAGE));
	}

	@Override
	public Endpoint createClusterService() {
		throw new NotImplementedError();
	}

	@Override
	public void createClusterPod(ResourceProfile resourceProfile) throws KubernetesClientException {
		final ResourceID resourceID = ResourceID.generate();
		final String podName = getPodName(resourceID);
		LOG.info("Creating a cluster pod [{}] for a resource profile [{}]", podName, resourceProfile);
		//TODO: Place template into a resource file or somewhere into config file
		V1Pod body = new V1Pod()
			.apiVersion("v1")
			.kind("Pod")
			.metadata(
				new V1ObjectMeta()
					.name(podName)
					.labels(new HashMap<String, String>() {{
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
				//TODO: Add resource spec (CPU, Memory) and an option to turn the feature on/off
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
		} catch (ApiException e) {
			final String message = String.format("Cannot create a pod for resource profile [%s]", resourceProfile);
			throw new KubernetesClientException(message, e);
		}
	}

	/**
	 * Terminates a cluster pod.
	 * @param resourceID cluster pod id in terms of flink
	 * @throws KubernetesClientException
	 */
	@Override
	public void terminateClusterPod(ResourceID resourceID) throws KubernetesClientException {
		final String podName = getPodName(resourceID);
		LOG.info("Terminating a cluster pod [{}] for a resource profile [{}]", podName, resourceID);
		deleteNamespacedPod(podName);
		podResourceProfiles.remove(podName);
	}

	/**
	 * Stops and cleans up a cluster.
	 * @param clusterId cluster id
	 */
	@Override
	public void stopAndCleanupCluster(String clusterId) {
		LOG.info("Stopping the cluster and deleting all its task manager pods");
		podResourceProfiles.forEach(
			(podName, resourceProfile) -> {
				try {
					deleteNamespacedPod(podName);
				} catch (KubernetesClientException e) {
					LOG.warn("Could not delete a pod [{}]", podName);
				}
			}
		);
		podResourceProfiles.clear();
	}

	@Override
	public Endpoint getResetEndpoint(String flinkClusterId) {
		throw new NotImplementedError();
	}

	@Override
	public void close() {
		throw new NotImplementedError();
	}

	private String getPodName(ResourceID resourceId) {
		return "flink-taskmanager-" + resourceId.getResourceIdString();
	}

	private void deleteNamespacedPod(String podName) throws KubernetesClientException {
		try {
			V1DeleteOptions body = new V1DeleteOptions().gracePeriodSeconds(0L).orphanDependents(false);
			coreV1Api.deleteNamespacedPod(podName, flinkNamespace, body, null, null, null, null, null);
		} catch (ApiException e) {
			if (e.getMessage().equals("Not Found")) {
				LOG.warn("Could not delete a pod [{}] as it was not found", podName);
			} else {
				final String message =
					String.format("Could not delete a pod [%s] for resource profile [%s]", podName, flinkNamespace);
				throw new KubernetesClientException(message, e);
			}
		} catch (JsonSyntaxException e) {
			// It's a known issue until the Swagger spec is updated to OpenAPI 3.0
			// https://github.com/kubernetes-client/java/issues/86
			// Simply ignoring the exception
		}
	}
}
