# kuberesolver

[![Go Reference](https://pkg.go.dev/badge/github.com/sercand/kuberesolver/v6.svg)](https://pkg.go.dev/github.com/sercand/kuberesolver)
[![Go Report Card](https://goreportcard.com/badge/github.com/sercand/kuberesolver/v6)](https://goreportcard.com/report/github.com/sercand/kuberesolver/v6)
[![GitHub release](https://img.shields.io/github/v/release/sercand/kuberesolver)](https://github.com/sercand/kuberesolver/releases)

A Grpc name resolver by using kubernetes API.
It comes with a small ~250 LOC kubernetes client to find service endpoints. Therefore it won't bloat your binaries.


### USAGE

```go

// Import the module
import "github.com/sercand/kuberesolver/v6"
	
// Register kuberesolver to grpc before calling grpc.NewClient
kuberesolver.RegisterInCluster()

// it is same as
resolver.Register(kuberesolver.NewBuilder(nil /*custom kubernetes client*/ , "kubernetes"))

// if schema is 'kubernetes' then grpc will use kuberesolver to resolve addresses
cc, err := grpc.NewClient("kubernetes:///service.namespace:portname", opts...)
```

An url can be one of the following, [grpc naming docs](https://github.com/grpc/grpc/blob/master/doc/naming.md)

```
kubernetes:///service-name:8080
kubernetes:///service-name:portname
kubernetes:///service-name.namespace:8080
kubernetes:///service-name.namespace.svc.cluster_name
kubernetes:///service-name.namespace.svc.cluster_name:8080

kubernetes://namespace/service-name:8080
kubernetes://service-name:8080/
kubernetes://service-name.namespace:8080/
kubernetes://service-name.namespace.svc.cluster_name
kubernetes://service-name.namespace.svc.cluster_name:8080
```
_* Please note that the cluster_name is not used in resolving the endpoints of a Service. It is only there to support fully qualified service names, e.g._ `test.default.svc.cluster.local`.

### Using alternative Schema

Use `RegisterInClusterWithSchema(schema)` instead of `RegisterInCluster` on start.

### Client Side Load Balancing

You need to pass [loadBalancingPolicy](https://github.com/grpc/grpc-go/blob/master/examples/features/load_balancing/README.md) option to grpc when setting up a new client: 

```go
grpc.NewClient(
    "kubernetes:///service:grpc", 
    grpc.WithDefaultServiceConfig(`{"loadBalancingPolicy":"round_robin"}`),
    grpc.WithTransportCredentials(insecure.NewCredentials()),
)
```
This will create subconnections for each available service endpoints.

### How is this different from dialing to `service.namespace:8080`

Connecting to a service by dialing to `service.namespace:8080` uses DNS and it returns service stable IP. Therefore, gRPC doesn't know the endpoint IP addresses and it fails to reconnect to target services in case of failure.  

Kuberesolver uses kubernetes API to get and watch service endpoint IP addresses. 
Since it provides and updates all available service endpoints, together with a client-side balancer you can achive zero downtime deployments.

### RBAC

If you use RBAC in your cluster, the service account needs the `list` and `watch` verbs on `endpointslices`:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: kuberesolver
rules:
  - apiGroups: ["discovery.k8s.io"]
    resources: ["endpointslices"]
    verbs: ["list", "watch"]
```

Earlier versions of this README asked for `get` and `watch` only. Without `list`, kuberesolver still resolves through the watch alone, but it logs an error on every reconnect, and it keeps the addresses of an EndpointSlice deleted while the watch was disconnected for as long as the process runs.

### Services with several EndpointSlices

Kubernetes splits a Service into several EndpointSlices when it has more than 100 endpoints (by default), and dual-stack Services have one EndpointSlice per IP family. kuberesolver merges all of them and gives gRPC every ready address once. An endpoint counts as ready when its `ready` condition is true or not set, as the Kubernetes API defines it. A dual-stack pod therefore gets two addresses, one IPv4 and one IPv6, and gRPC opens a connection to each.

### Metrics

Each target exports three Prometheus gauges, labelled with `target`:

| Metric | Meaning |
| --- | --- |
| `kuberesolver_endpoints_total` | Endpoints across all EndpointSlices of the Service, ready or not. An endpoint can be counted twice for a moment while Kubernetes moves it between slices. |
| `kuberesolver_addresses_total` | Unique ready addresses known for the Service. When it drops to 0, gRPC keeps the last non-empty list. |
| `kuberesolver_client_last_update` | Unix time of the last time gRPC was confirmed to have the current addresses. |

### Using With TLS

You need to a certificate with name `service-name.namespace` in order to connect with TLS to your services.
