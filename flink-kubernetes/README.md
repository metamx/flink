# Flink On Kubernetes

## Job manager
Job manager is run as a pod of corresponding deployment which is supposed to be prepared beforehand. 
To create the deployment and a service for it the following templates (under `flink-kubernets/templates` path) 
should be used:
```
jobmanager-deployment.yaml
jobmanager-service.yaml
```
Example:
```
kubectl create -f jobmanager-deployment.yaml
kubectl create -f jobmanager-service.yaml
```
That creates the deployment with one job manager and a service around it that exposes the job manager.

## Task Manager
Task manager is a temporary essence and is created (and deleted) by a job manager for a particular slot. 
No deployments/jobs/services are created for a task manager only pods. 
A template for a task manager is hardcoded into the implementation 
(`org.apache.flink.kubernetes.client.DefaultKubernetesClient`).

For every slot request the job manager passes a resource profile to a resource manager 
(`org.apache.flink.kubernetes.KubernetesResourceManager`). The resource profile contains specific hardware requirements
(CPU, Memory and other). All these requirements are included into the pod template as labels thus they could be used for 
binding specific pods onto specific VMs.

## Resource Profile
A resource profile might be set to a `StreamTransformation` by calling a corresponding method. A resource profile has 
cpu cores, heap memory, direct memory, native memory, network memory and extended resources (GPU and user defined).
Only `StreamTransformation.minResources` is used for a pod template.

### Resource Profile Configuration Example
TBD

## Kubernetes Resource Management
Resource management uses a default service account every pod contains. It should has admin privileges to be able 
to create and delete pods:
```
kubectl create clusterrolebinding serviceaccounts-cluster-admin \
     --clusterrole=cluster-admin \
     --group=system:serviceaccounts
``` 

## Build and run
The implementation is based on existing mechanism of packaging (maven) and containerization (docker). 

Prepare a package first:
```mvn clean package -DskipTests```
Then in needs to be containerized:
```cd flink-contrib/docker-flink/
sh build.sh --from-local-dist --image-name flink-mmx
```
If minikube is used then a container image should be uploaded into minikube node. 
So before building a container image a docker env is supposed to be exported:
```
eval $(minikube docker-env)
```
Job manager deployment and service:
```
cd ../../flink-kubernetes/templates/
kubectl create -f jobmanager-deployment.yaml
kubectl create -f jobmanager-service.yaml
```
