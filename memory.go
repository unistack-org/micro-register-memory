// Package memory provides an in-memory registry
package memory

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/unistack-org/micro/v3/logger"
	"github.com/unistack-org/micro/v3/registry"
)

var (
	sendEventTime = 10 * time.Millisecond
	ttlPruneTime  = time.Second
)

type node struct {
	*registry.Node
	TTL      time.Duration
	LastSeen time.Time
}

type record struct {
	Name      string
	Version   string
	Metadata  map[string]string
	Nodes     map[string]*node
	Endpoints []*registry.Endpoint
}

type memory struct {
	opts registry.Options
	// records is a KV map with domain name as the key and a services map as the value
	records  map[string]services
	watchers map[string]*Watcher
	sync.RWMutex
}

// services is a KV map with service name as the key and a map of records as the value
type services map[string]map[string]*record

// NewRegistry returns an initialized in-memory registry
func NewRegistry(opts ...registry.Option) registry.Registry {
	r := &memory{
		opts:     registry.NewOptions(opts...),
		records:  make(map[string]services),
		watchers: make(map[string]*Watcher),
	}

	go r.ttlPrune()

	return r
}

func (m *memory) ttlPrune() {
	prune := time.NewTicker(ttlPruneTime)
	defer prune.Stop()

	for {
		select {
		case <-prune.C:
			m.Lock()
			for domain, services := range m.records {
				for service, versions := range services {
					for version, record := range versions {
						for id, n := range record.Nodes {
							if n.TTL != 0 && time.Since(n.LastSeen) > n.TTL {
								if m.opts.Logger.V(logger.DebugLevel) {
									m.opts.Logger.Debugf(m.opts.Context, "Registry TTL expired for node %s of service %s", n.Id, service)
								}
								delete(m.records[domain][service][version].Nodes, id)
							}
						}
					}
				}
			}
			m.Unlock()
		}
	}
}

func (m *memory) sendEvent(r *registry.Result) {
	m.RLock()
	watchers := make([]*Watcher, 0, len(m.watchers))
	for _, w := range m.watchers {
		watchers = append(watchers, w)
	}
	m.RUnlock()

	for _, w := range watchers {
		select {
		case <-w.exit:
			m.Lock()
			delete(m.watchers, w.id)
			m.Unlock()
		default:
			select {
			case w.res <- r:
			case <-time.After(sendEventTime):
			}
		}
	}
}

func (m *memory) Connect(ctx context.Context) error {
	return nil
}

func (m *memory) Disconnect(ctx context.Context) error {
	return nil
}

func (m *memory) Init(opts ...registry.Option) error {
	for _, o := range opts {
		o(&m.opts)
	}

	// add services
	m.Lock()
	defer m.Unlock()

	return nil
}

func (m *memory) Options() registry.Options {
	return m.opts
}

func (m *memory) Register(ctx context.Context, s *registry.Service, opts ...registry.RegisterOption) error {
	m.Lock()
	defer m.Unlock()

	options := registry.NewRegisterOptions(opts...)

	// get the services for this domain from the registry
	srvs, ok := m.records[options.Domain]
	if !ok {
		srvs = make(services)
	}

	// domain is set in metadata so it can be passed to watchers
	if s.Metadata == nil {
		s.Metadata = map[string]string{"domain": options.Domain}
	} else {
		s.Metadata["domain"] = options.Domain
	}

	// ensure the service name exists
	r := serviceToRecord(s, options.TTL)
	if _, ok := srvs[s.Name]; !ok {
		srvs[s.Name] = make(map[string]*record)
	}

	if _, ok := srvs[s.Name][s.Version]; !ok {
		srvs[s.Name][s.Version] = r
		if m.opts.Logger.V(logger.DebugLevel) {
			m.opts.Logger.Debugf(m.opts.Context, "Registry added new service: %s, version: %s", s.Name, s.Version)
		}
		m.records[options.Domain] = srvs
		go m.sendEvent(&registry.Result{Action: "create", Service: s})
	}

	var addedNodes bool

	for _, n := range s.Nodes {
		// check if already exists
		if _, ok := srvs[s.Name][s.Version].Nodes[n.Id]; ok {
			continue
		}

		metadata := make(map[string]string)

		// make copy of metadata
		for k, v := range n.Metadata {
			metadata[k] = v
		}

		// set the domain
		metadata["domain"] = options.Domain

		// add the node
		srvs[s.Name][s.Version].Nodes[n.Id] = &node{
			Node: &registry.Node{
				Id:       n.Id,
				Address:  n.Address,
				Metadata: metadata,
			},
			TTL:      options.TTL,
			LastSeen: time.Now(),
		}

		addedNodes = true
	}

	if addedNodes {
		if m.opts.Logger.V(logger.DebugLevel) {
			m.opts.Logger.Debugf(m.opts.Context, "Registry added new node to service: %s, version: %s", s.Name, s.Version)
		}
		go m.sendEvent(&registry.Result{Action: "update", Service: s})
	} else {
		// refresh TTL and timestamp
		for _, n := range s.Nodes {
			if m.opts.Logger.V(logger.DebugLevel) {
				m.opts.Logger.Debugf(m.opts.Context, "Updated registration for service: %s, version: %s", s.Name, s.Version)
			}
			srvs[s.Name][s.Version].Nodes[n.Id].TTL = options.TTL
			srvs[s.Name][s.Version].Nodes[n.Id].LastSeen = time.Now()
		}
	}

	m.records[options.Domain] = srvs
	return nil
}

func (m *memory) Deregister(ctx context.Context, s *registry.Service, opts ...registry.DeregisterOption) error {
	m.Lock()
	defer m.Unlock()

	options := registry.NewDeregisterOptions(opts...)

	// domain is set in metadata so it can be passed to watchers
	if s.Metadata == nil {
		s.Metadata = map[string]string{"domain": options.Domain}
	} else {
		s.Metadata["domain"] = options.Domain
	}

	// if the domain doesn't exist, there is nothing to deregister
	services, ok := m.records[options.Domain]
	if !ok {
		return nil
	}

	// if no services with this name and version exist, there is nothing to deregister
	versions, ok := services[s.Name]
	if !ok {
		return nil
	}

	version, ok := versions[s.Version]
	if !ok {
		return nil
	}

	// deregister all of the service nodes from this version
	for _, n := range s.Nodes {
		if _, ok := version.Nodes[n.Id]; ok {
			if m.opts.Logger.V(logger.DebugLevel) {
				m.opts.Logger.Debugf(m.opts.Context, "Registry removed node from service: %s, version: %s", s.Name, s.Version)
			}
			delete(version.Nodes, n.Id)
		}
	}

	// if the nodes not empty, we replace the version in the store and exist, the rest of the logic
	// is cleanup
	if len(version.Nodes) > 0 {
		m.records[options.Domain][s.Name][s.Version] = version
		go m.sendEvent(&registry.Result{Action: "update", Service: s})
		return nil
	}

	// if this version was the only version of the service, we can remove the whole service from the
	// registry and exit
	if len(versions) == 1 {
		delete(m.records[options.Domain], s.Name)
		go m.sendEvent(&registry.Result{Action: "delete", Service: s})

		if m.opts.Logger.V(logger.DebugLevel) {
			m.opts.Logger.Debugf(m.opts.Context, "Registry removed service: %s", s.Name)
		}
		return nil
	}

	// there are other versions of the service running, so only remove this version of it
	delete(m.records[options.Domain][s.Name], s.Version)
	go m.sendEvent(&registry.Result{Action: "delete", Service: s})
	if m.opts.Logger.V(logger.DebugLevel) {
		m.opts.Logger.Debugf(m.opts.Context, "Registry removed service: %s, version: %s", s.Name, s.Version)
	}

	return nil
}

func (m *memory) GetService(ctx context.Context, name string, opts ...registry.GetOption) ([]*registry.Service, error) {
	options := registry.NewGetOptions(opts...)

	// if it's a wildcard domain, return from all domains
	if options.Domain == registry.WildcardDomain {
		m.RLock()
		recs := m.records
		m.RUnlock()

		var services []*registry.Service

		for domain := range recs {
			srvs, err := m.GetService(ctx, name, append(opts, registry.GetDomain(domain))...)
			if err == registry.ErrNotFound {
				continue
			} else if err != nil {
				return nil, err
			}
			services = append(services, srvs...)
		}

		if len(services) == 0 {
			return nil, registry.ErrNotFound
		}
		return services, nil
	}

	m.RLock()
	defer m.RUnlock()

	// check the domain exists
	services, ok := m.records[options.Domain]
	if !ok {
		return nil, registry.ErrNotFound
	}

	// check the service exists
	versions, ok := services[name]
	if !ok || len(versions) == 0 {
		return nil, registry.ErrNotFound
	}

	// serialize the response
	result := make([]*registry.Service, len(versions))

	var i int

	for _, r := range versions {
		result[i] = recordToService(r, options.Domain)
		i++
	}

	return result, nil
}

func (m *memory) ListServices(ctx context.Context, opts ...registry.ListOption) ([]*registry.Service, error) {
	options := registry.NewListOptions(opts...)

	// if it's a wildcard domain, list from all domains
	if options.Domain == registry.WildcardDomain {
		m.RLock()
		recs := m.records
		m.RUnlock()

		var services []*registry.Service

		for domain := range recs {
			srvs, err := m.ListServices(ctx, append(opts, registry.ListDomain(domain))...)
			if err != nil {
				return nil, err
			}
			services = append(services, srvs...)
		}

		return services, nil
	}

	m.RLock()
	defer m.RUnlock()

	// ensure the domain exists
	services, ok := m.records[options.Domain]
	if !ok {
		return make([]*registry.Service, 0), nil
	}

	// serialize the result, each version counts as an individual service
	var result []*registry.Service

	for domain, service := range services {
		for _, version := range service {
			result = append(result, recordToService(version, domain))
		}
	}

	return result, nil
}

func (m *memory) Watch(ctx context.Context, opts ...registry.WatchOption) (registry.Watcher, error) {
	wo := registry.NewWatchOptions(opts...)

	// construct the watcher
	w := &Watcher{
		exit: make(chan bool),
		res:  make(chan *registry.Result),
		id:   uuid.New().String(),
		wo:   wo,
	}

	m.Lock()
	m.watchers[w.id] = w
	m.Unlock()

	return w, nil
}

func (m *memory) String() string {
	return "memory"
}
