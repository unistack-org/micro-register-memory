package memory

import (
	"time"

	"github.com/unistack-org/micro/v3/register"
)

func serviceToRecord(s *register.Service, ttl time.Duration) *record {
	metadata := make(map[string]string, len(s.Metadata))
	for k, v := range s.Metadata {
		metadata[k] = v
	}

	nodes := make(map[string]*node, len(s.Nodes))
	for _, n := range s.Nodes {
		nodes[n.Id] = &node{
			Node:     n,
			TTL:      ttl,
			LastSeen: time.Now(),
		}
	}

	endpoints := make([]*register.Endpoint, len(s.Endpoints))
	for i, e := range s.Endpoints {
		endpoints[i] = e
	}

	return &record{
		Name:      s.Name,
		Version:   s.Version,
		Metadata:  metadata,
		Nodes:     nodes,
		Endpoints: endpoints,
	}
}

func recordToService(r *record, domain string) *register.Service {
	metadata := make(map[string]string, len(r.Metadata))
	for k, v := range r.Metadata {
		metadata[k] = v
	}

	// set the domain in metadata so it can be determined when a wildcard query is performed
	metadata["domain"] = domain

	endpoints := make([]*register.Endpoint, len(r.Endpoints))
	for i, e := range r.Endpoints {
		request := new(register.Value)
		if e.Request != nil {
			*request = *e.Request
		}
		response := new(register.Value)
		if e.Response != nil {
			*response = *e.Response
		}

		metadata := make(map[string]string, len(e.Metadata))
		for k, v := range e.Metadata {
			metadata[k] = v
		}

		endpoints[i] = &register.Endpoint{
			Name:     e.Name,
			Request:  request,
			Response: response,
			Metadata: metadata,
		}
	}

	nodes := make([]*register.Node, len(r.Nodes))
	i := 0
	for _, n := range r.Nodes {
		metadata := make(map[string]string, len(n.Metadata))
		for k, v := range n.Metadata {
			metadata[k] = v
		}

		nodes[i] = &register.Node{
			Id:       n.Id,
			Address:  n.Address,
			Metadata: metadata,
		}
		i++
	}

	return &register.Service{
		Name:      r.Name,
		Version:   r.Version,
		Metadata:  metadata,
		Endpoints: endpoints,
		Nodes:     nodes,
	}
}
