package kuberesolver

type EventType string

const (
	Added    EventType = "ADDED"
	Modified EventType = "MODIFIED"
	Deleted  EventType = "DELETED"
	Error    EventType = "ERROR"
)

// Event represents a single event to a watched resource.
type Event struct {
	Type   EventType     `json:"type"`
	Object EndpointSlice `json:"object"`
	// Status is the error an ERROR event carries instead of an EndpointSlice.
	Status Status `json:"-"`
}

// Status is the error the API server sends in an ERROR watch event, for
// example code 410 when the requested resourceVersion is too old.
type Status struct {
	Code    int    `json:"code"`
	Reason  string `json:"reason"`
	Message string `json:"message"`
}

type EndpointSliceList struct {
	Metadata ListMetadata `json:"metadata"`
	Items    []EndpointSlice
}

// ListMetadata is the part of a list's metadata the resolver reads.
type ListMetadata struct {
	ResourceVersion string `json:"resourceVersion"`
}

type EndpointSlice struct {
	Metadata  Metadata `json:"metadata"`
	Endpoints []Endpoint
	Ports     []EndpointPort
}

type Endpoint struct {
	Addresses  []string
	Conditions EndpointConditions
}

type EndpointConditions struct {
	Ready       *bool `json:"ready"`
	Serving     *bool `json:"serving"`
	Terminating *bool `json:"terminating"`
}

type Metadata struct {
	Name            string            `json:"name"`
	Namespace       string            `json:"namespace"`
	ResourceVersion string            `json:"resourceVersion"`
	Labels          map[string]string `json:"labels"`
}
type EndpointPort struct {
	Name string `json:"name"`
	Port int    `json:"port"`
}
