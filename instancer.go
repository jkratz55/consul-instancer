package instancer

import (
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/consul/api"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

// Listener is a type that handles events from Instancer. A Listener can be registered
// with an instance of Instancer to receive updates via the OnEvent method. Implementations
// can store the instances as updates occur of process them however they please.
type Listener interface {
	OnEvent(instances []string)
}

// defaultIndex is the default WaitIndex used to query Consul
const defaultIndex = 0

// ErrNoInstancesAvailable is an error returned when Instancer has no instances
// available.
var ErrNoInstancesAvailable = errors.New("no instances available")

// Instancer is a type that yields instances of a service registered in Consul.
// Instancer will watch for changes in Consul and update its instances cached
// accordingly.
type Instancer struct {
	client      *api.Client
	logger      Logger
	service     string
	tags        map[string]struct{}
	passingOnly bool
	quitc       chan struct{}
	lastIndex   uint64
	instances   []string
	listeners   []Listener
	mutex       sync.RWMutex
	counter     uint64
}

// NewInstancer initializes and returns a new Instancer. Upon initialization it
// will query consul to get the initial set of instances available and begin listening
// for events on a background goroutine.
func NewInstancer(client *api.Client,
	logger Logger,
	service string,
	tags []string,
	passingOnly bool) *Instancer {

	tagMap := make(map[string]struct{})
	for _, tag := range tags {
		tagMap[tag] = struct{}{}
	}

	i := &Instancer{
		client:      client,
		logger:      logger,
		service:     service,
		tags:        tagMap,
		passingOnly: passingOnly,
		quitc:       make(chan struct{}),
		lastIndex:   defaultIndex,
		instances:   make([]string, 0),
		listeners:   make([]Listener, 0),
		mutex:       sync.RWMutex{},
		counter:     0,
	}

	instances, lastIndex, err := i.getInstances(0)
	if err != nil {
		i.logger.Log("error querying Consul: %w", err)
	} else {
		i.logger.Log("successfully queried Consul for service %s: Instances: %s", i.service, instances)
	}

	i.instances = instances
	i.lastIndex = lastIndex

	go i.loop()
	return i
}

// Register registers a listener to receive updates from Instancer
// as it updates its internal set of instances from Consul events.
func (i *Instancer) Register(l Listener) {
	i.mutex.Lock()
	i.listeners = append(i.listeners, l)
	i.mutex.Unlock()
}

// Instances returns the current set of instances
func (i *Instancer) Instances() []string {
	i.mutex.RLock()
	defer i.mutex.RUnlock()
	return i.instances
}

// Instance returns a single instance round-robin load balanced. If
// no instances are available a non nil error value will be returned.
//
// Returns ErrNoInstancesAvailable when there are no instances available
func (i *Instancer) Instance() (string, error) {
	i.mutex.RLock()
	defer i.mutex.RUnlock()

	if len(i.instances) == 0 {
		return "", ErrNoInstancesAvailable
	}
	old := atomic.AddUint64(&i.counter, 1) - 1
	idx := old % uint64(len(i.instances))
	return i.instances[idx], nil
}

// loop runs a for loop forever which queries Consul using wait queries to get
// updates on the health of the service
func (i *Instancer) loop() {
	// The Duration to wait when an error occurs so we don't bombard Consul
	backoff := 500 * time.Millisecond
	for {
		// Query consul for instances
		instances, index, err := i.getInstances(i.lastIndex)
		// If an error occurs sleep for the duration and increase the backoff to
		// prevent hammering Consul, then skip to next iteration
		if err != nil {
			i.logger.Log("error querying Consul: %w", err)
			time.Sleep(backoff)
			backoff = exponential(backoff)
			continue
		}
		// If the index returned from querying consul is less than the last known index
		// then something has gone sideway with the state of Consul or Instancer, reset
		// to default index and go to next iteration.
		if index < i.lastIndex {
			i.logger.Log("index returned from Consul is not sane, value is lower than previous index")
			i.logger.Log("resetting index to 0")
			i.lastIndex = defaultIndex
			continue
		}
		// Update the internal state of instancer with the index returned from consul
		// and the instances
		i.mutex.Lock()
		i.lastIndex = index
		i.instances = instances
		i.mutex.Unlock()

		// Reset the backoff
		backoff = 500 * time.Millisecond
		i.logger.Log("refreshed instances: %s", instances)

		// Notify the listeners of the change
		for _, listener := range i.listeners {
			listener.OnEvent(instances)
		}
	}
}

// getInstances queries the health information of a service in Consul and returns
// the instances and last index. If the operation fails a non nil error value will
// be returned.
func (i *Instancer) getInstances(index uint64) ([]string, uint64, error) {

	var (
		wg      sync.WaitGroup
		entries []*api.ServiceEntry
		meta    *api.QueryMeta
		err     error
	)

	wg.Add(1)
	go func() {
		defer wg.Done()
		entries, meta, err = i.client.Health().Service(i.service, "", i.passingOnly, &api.QueryOptions{
			WaitIndex: index,
		})
	}()
	wg.Wait()

	if err != nil {
		return nil, 0, err
	}

	if len(i.tags) > 0 {
		entries = filterServiceEntries(entries, i.tags)
	}

	instances := makeInstances(entries)
	return instances, meta.LastIndex, nil
}

func filterServiceEntries(entries []*api.ServiceEntry, tags map[string]struct{}) []*api.ServiceEntry {
	filteredEntries := make([]*api.ServiceEntry, 0)
	for _, entry := range entries {
		// Build map of tags the service has
		serviceTags := make(map[string]struct{}, len(entry.Service.Tags))
		for _, tag := range entry.Service.Tags {
			serviceTags[tag] = struct{}{}
		}

		for tag := range tags {
			if _, ok := serviceTags[tag]; ok {
				filteredEntries = append(filteredEntries, entry)
			}
		}
	}
	return filteredEntries
}

func makeInstances(entries []*api.ServiceEntry) []string {
	instances := make([]string, len(entries))
	for i, entry := range entries {
		addr := entry.Node.Address
		if entry.Service.Address != "" {
			addr = entry.Service.Address
		}
		instances[i] = fmt.Sprintf("%s:%d", addr, entry.Service.Port)
	}
	return instances
}

func exponential(d time.Duration) time.Duration {
	d *= 2
	jitter := rand.Float64() + 0.5
	d = time.Duration(int64(float64(d.Nanoseconds()) * jitter))
	if d > time.Minute {
		d = time.Minute
	}
	return d
}
