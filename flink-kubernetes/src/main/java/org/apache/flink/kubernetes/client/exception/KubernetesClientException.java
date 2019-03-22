package org.apache.flink.kubernetes.client.exception;

public class KubernetesClientException extends Exception
{
	public KubernetesClientException(String message) {
		super(message);
	}

	public KubernetesClientException(String message, Throwable cause) {
		super(message, cause);
	}
}
