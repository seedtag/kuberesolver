package kuberesolver

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/resolver"
)

const (
	kubernetesSchema = "kubernetes"
	// listTimeout bounds a LIST, so a hung request can't stop the resolver
	// from handling watch events.
	listTimeout = 30 * time.Second
)

// errWatchEvent is returned when the API server ends a watch with an ERROR
// event, usually 410 Gone because the resourceVersion is too old.
var errWatchEvent = errors.New("kuberesolver: watch returned an ERROR event")

var (
	endpointsForTarget = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "kuberesolver_endpoints_total",
			Help: "Endpoints across all EndpointSlices of the target's Service, ready or not",
		},
		[]string{"target"},
	)
	addressesForTarget = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "kuberesolver_addresses_total",
			Help: "Unique ready addresses known for the target's Service",
		},
		[]string{"target"},
	)
	clientLastUpdate = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "kuberesolver_client_last_update",
			Help: "Unix time when gRPC was last confirmed to have the target's current addresses",
		},
		[]string{"target"},
	)
)

type targetInfo struct {
	scheme            string
	serviceName       string
	serviceNamespace  string
	port              string
	resolveByPortName bool
	useFirstPort      bool
}

func (ti targetInfo) String() string {
	if ti.scheme != "" {
		return fmt.Sprintf("%s://%s/%s:%s", ti.scheme, ti.serviceNamespace, ti.serviceName, ti.port)
	} else {
		return fmt.Sprintf("kubernetes://%s/%s:%s", ti.serviceNamespace, ti.serviceName, ti.port)
	}
}

// RegisterInCluster registers the kuberesolver builder to grpc with kubernetes schema
func RegisterInCluster() {
	RegisterInClusterWithSchema(kubernetesSchema)
}

// RegisterInClusterWithSchema registers the kuberesolver builder to the grpc with custom schema
func RegisterInClusterWithSchema(schema string) {
	resolver.Register(NewBuilder(nil, schema))
}

// NewBuilder creates a kubeBuilder which is used by grpc resolver.
func NewBuilder(client K8sClient, schema string) resolver.Builder {
	return &kubeBuilder{
		k8sClient: client,
		schema:    schema,
	}
}

type kubeBuilder struct {
	k8sClient K8sClient
	schema    string
}

func splitServicePortNamespace(hpn string) (service, port, namespace string) {
	service = hpn

	colon := strings.LastIndexByte(service, ':')
	if colon != -1 {
		service, port = service[:colon], service[colon+1:]
	}

	// we want to split into the service name, namespace, and whatever else is left
	// this will support fully qualified service names, e.g. {service-name}.<namespace>.svc.<cluster-domain-name>.
	// Note that since we lookup the endpoints by service name and namespace, we don't care about the
	// cluster-domain-name, only that we can parse out the service name and namespace properly.
	parts := strings.SplitN(service, ".", 3)
	if len(parts) >= 2 {
		service, namespace = parts[0], parts[1]
	}

	return
}

func parseResolverTarget(target resolver.Target) (targetInfo, error) {
	var service, port, namespace string
	if target.URL.Host == "" {
		// kubernetes:///service.namespace:port
		service, port, namespace = splitServicePortNamespace(target.Endpoint())
	} else if target.URL.Port() == "" && target.Endpoint() != "" {
		// kubernetes://namespace/service:port
		service, port, _ = splitServicePortNamespace(target.Endpoint())
		namespace = target.URL.Hostname()
	} else {
		// kubernetes://service.namespace:port
		service, port, namespace = splitServicePortNamespace(target.URL.Host)
	}

	if service == "" {
		return targetInfo{}, fmt.Errorf("target %s must specify a service", &target.URL)
	}

	resolveByPortName := false

	useFirstPort := false
	if port == "" {
		useFirstPort = true
	} else if _, err := strconv.Atoi(port); err != nil {
		resolveByPortName = true
	}

	return targetInfo{
		scheme:            target.URL.Scheme,
		serviceName:       service,
		serviceNamespace:  namespace,
		port:              port,
		resolveByPortName: resolveByPortName,
		useFirstPort:      useFirstPort,
	}, nil
}

// Build creates a new resolver for the given target.
//
// gRPC dial calls Build synchronously, and fails if the returned error is
// not nil.
func (b *kubeBuilder) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	if b.k8sClient == nil {
		if cl, err := NewInClusterK8sClient(); err == nil {
			b.k8sClient = cl
		} else {
			return nil, err
		}
	}

	ti, err := parseResolverTarget(target)
	if err != nil {
		return nil, err
	}

	if ti.serviceNamespace == "" {
		ti.serviceNamespace = getCurrentNamespaceOrDefault()
	}

	ctx, cancel := context.WithCancel(context.Background())
	r := &kResolver{
		target:    ti,
		cancel:    cancel,
		cc:        cc,
		k8sClient: b.k8sClient,
		slices:    map[string]EndpointSlice{},

		endpoints:      endpointsForTarget.WithLabelValues(ti.String()),
		addresses:      addressesForTarget.WithLabelValues(ti.String()),
		lastUpdateUnix: clientLastUpdate.WithLabelValues(ti.String()),
	}
	r.wg.Add(1)

	go func() {
		// Done once, when the whole retry loop exits, not after each watch
		// attempt: Close must wait until no attempt can run anymore.
		defer r.wg.Done()

		until(func() {
			err := r.watch(ctx)
			// Errors caused by Close are expected and not worth a log line.
			if err != nil && !errors.Is(err, io.EOF) && ctx.Err() == nil {
				grpclog.Errorf("kuberesolver: watching ended with error='%v', will reconnect again", err)
			}
		}, time.Second, time.Second*30, ctx.Done())
	}()

	return r, nil
}

// Scheme returns the scheme supported by this resolver.
// Scheme is defined at https://github.com/grpc/grpc/blob/master/doc/naming.md.
func (b *kubeBuilder) Scheme() string {
	return b.schema
}

type kResolver struct {
	target    targetInfo
	cancel    context.CancelFunc
	cc        resolver.ClientConn
	k8sClient K8sClient
	// wg is used to enforce Close() to return after the watcher() goroutine has finished.
	wg sync.WaitGroup

	// slices holds the latest version of every EndpointSlice of the Service,
	// keyed by slice name. A Service can be split over several slices (more
	// than ~100 endpoints, dual-stack), and gRPC must get the union of all of
	// them. Only the watch goroutine reads or writes it.
	slices map[string]EndpointSlice

	// pushed is the address list last sent to gRPC. Sending gRPC the same
	// list again costs it O(addresses) or worse, so update skips it.
	pushed []resolver.Address

	// missingPort is whether a slice lacked the named target port on the
	// last update, so the warning is logged once, not on every event.
	missingPort bool

	endpoints prometheus.Gauge
	addresses prometheus.Gauge
	// lastUpdateUnix is the last time the resolver confirmed that gRPC has
	// the current addresses, whether or not that needed a new push.
	lastUpdateUnix prometheus.Gauge
}

// ResolveNow will be called by gRPC to try to resolve the target name again.
// It's just a hint, resolver can ignore this if it's not necessary.
func (k *kResolver) ResolveNow(resolver.ResolveNowOptions) {
	// ignore
}

// Close closes the resolver.
func (k *kResolver) Close() {
	k.cancel()
	k.wg.Wait()
}

// makeAddresses returns the ready addresses of a slice. ok is false when the
// target names a port the slice doesn't have.
func (k *kResolver) makeAddresses(e EndpointSlice) (addrs []resolver.Address, ok bool) {
	if len(e.Endpoints) == 0 {
		return nil, true
	}

	// A numeric target port is used as is; otherwise take it from the slice.
	port := k.target.port

	switch {
	case k.target.useFirstPort:
		if len(e.Ports) == 0 {
			// No port from the target or the slice: nothing valid to publish.
			return nil, true
		}

		port = strconv.Itoa(e.Ports[0].Port)
	case k.target.resolveByPortName:
		i := slices.IndexFunc(e.Ports, func(p EndpointPort) bool { return p.Name == k.target.port })
		if i < 0 {
			return nil, false
		}

		port = strconv.Itoa(e.Ports[i].Port)
	}

	serverName := k.target.serviceName + "." + k.target.serviceNamespace
	newAddrs := make([]resolver.Address, 0, len(e.Endpoints))

	for _, endpoint := range e.Endpoints {
		// The API defines a missing ready condition as ready.
		if endpoint.Conditions.Ready != nil && !*endpoint.Conditions.Ready {
			continue
		}

		for _, address := range endpoint.Addresses {
			newAddrs = append(newAddrs, resolver.Address{
				Addr:       net.JoinHostPort(address, port),
				ServerName: serverName,
			})
		}
	}

	return newAddrs, true
}

// applyEvent updates the slice cache from one watch event and pushes the
// merged addresses to gRPC.
func (k *kResolver) applyEvent(e Event) {
	switch e.Type {
	case Added, Modified:
		k.slices[e.Object.Metadata.Name] = e.Object
	case Deleted:
		delete(k.slices, e.Object.Metadata.Name)
	case Error:
		// watch handles ERROR events itself; this case keeps the switch
		// exhaustive. Other event types are rejected by streamWatcher.Decode.
		return
	}

	k.update()
}

// update pushes the union of the addresses of all cached slices to gRPC.
func (k *kResolver) update() {
	names := make([]string, 0, len(k.slices))
	endpoints := 0

	for name, s := range k.slices {
		names = append(names, name)
		endpoints += len(s.Endpoints)
	}
	// Sort so the address order is the same for the same set of slices.
	slices.Sort(names)

	addrs := make([]resolver.Address, 0, endpoints)
	seen := make(map[string]struct{}, endpoints)
	missingPort := false

	for _, name := range names {
		sliceAddrs, ok := k.makeAddresses(k.slices[name])
		if !ok {
			missingPort = true
		}

		for _, a := range sliceAddrs {
			// An endpoint can briefly be listed in two slices while the
			// EndpointSlice controller moves it.
			if _, ok := seen[a.Addr]; ok {
				continue
			}

			seen[a.Addr] = struct{}{}
			addrs = append(addrs, a)
		}
	}

	if missingPort && !k.missingPort {
		grpclog.Warningf("kuberesolver: %s: some EndpointSlices have no port named %q, their endpoints are skipped", k.target, k.target.port)

		// Never resolved and nothing to connect to: make RPCs fail fast
		// with this reason instead of waiting for their deadline.
		if len(addrs) == 0 && len(k.pushed) == 0 {
			k.cc.ReportError(fmt.Errorf("kuberesolver: no EndpointSlice of %s has a port named %q", k.target, k.target.port))
		}
	}

	k.missingPort = missingPort

	// With no ready addresses, keep the last state rather than leave gRPC
	// with nothing to connect to.
	if len(addrs) > 0 {
		if !sameAddrs(addrs, k.pushed) {
			if err := k.cc.UpdateState(resolver.State{Addresses: addrs}); err != nil {
				// Leave pushed as it is, so the next update sends these again.
				grpclog.Warningf("kuberesolver: gRPC rejected the addresses of %s: %v", k.target, err)
			} else {
				k.pushed = addrs
			}
		}

		if sameAddrs(addrs, k.pushed) {
			k.lastUpdateUnix.Set(float64(time.Now().Unix()))
		}
	}

	k.endpoints.Set(float64(endpoints))
	k.addresses.Set(float64(len(addrs)))
}

// sameAddrs reports whether two address lists are equal. Only Addr is
// compared: ServerName is the same for every address of a resolver.
func sameAddrs(a, b []resolver.Address) bool {
	return slices.EqualFunc(a, b, func(x, y resolver.Address) bool { return x.Addr == y.Addr })
}

// resolve lists all slices of the Service and replaces the cache with them, so
// slices whose DELETED event was missed while the watch was down are dropped.
// It returns the list's resourceVersion, or "" when the LIST failed. ctx is
// the resolver's context.
func (k *kResolver) resolve(ctx context.Context) string {
	listCtx, cancel := context.WithTimeout(ctx, listTimeout)
	defer cancel()

	list, err := getEndpointSliceList(listCtx, k.k8sClient, k.target.serviceNamespace, k.target.serviceName)
	if err != nil {
		var se *statusError

		switch {
		case ctx.Err() != nil:
			// The resolver is closing.
		case errors.As(err, &se) && se.code == http.StatusForbidden:
			grpclog.Errorf("kuberesolver: lookup endpoints failed, the service account needs the list verb on endpointslices: %v", err)
		default:
			grpclog.Errorf("kuberesolver: lookup endpoints failed: %v", err)
		}

		return ""
	}

	cache := make(map[string]EndpointSlice, len(list.Items))
	for _, s := range list.Items {
		cache[s.Metadata.Name] = s
	}

	k.slices = cache
	k.update()

	return list.Metadata.ResourceVersion
}

// watch runs one LIST-then-WATCH attempt until the watch ends or ctx is done.
func (k *kResolver) watch(ctx context.Context) error {
	// Start every (re)connect with a LIST, then watch from the list's
	// resourceVersion: the watch then reports every change after the LIST,
	// deletions included, and doesn't replay the slices the LIST returned.
	// If the LIST fails (for example without the list verb), fall back to a
	// watch without a version, which replays all current slices instead.
	rv := k.resolve(ctx)
	if ctx.Err() != nil {
		return nil
	}

	sw, err := watchEndpointSlice(ctx, k.k8sClient, k.target.serviceNamespace, k.target.serviceName, rv)
	if err != nil {
		return err
	}
	// Close the stream on every way out, including the ERROR event below,
	// so the connection and its reader goroutine don't outlive this attempt.
	defer sw.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case up, hasMore := <-sw.ResultChan():
			if !hasMore {
				return nil
			}

			if up.Type == Error {
				// End this watch; the next attempt starts with a new LIST.
				return fmt.Errorf("%w: %d %s: %s", errWatchEvent, up.Status.Code, up.Status.Reason, up.Status.Message)
			}

			k.applyEvent(up)
		}
	}
}
