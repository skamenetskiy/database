package database

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"

	roundrobin "github.com/skamenetskiy/round-robin"
	"gopkg.in/yaml.v3"
)

// NewCluster constructor.
func NewCluster(ctx context.Context, cfg ClusterConfig) (Cluster, error) {
	return newCluster(ctx, cfg)
}

// NewClusterFromFile reads config from configFilePath in json or yaml format and initializes a new Cluster.
func NewClusterFromFile(ctx context.Context, configFilePath string) (Cluster, error) {
	b, err := os.ReadFile(configFilePath)
	if err != nil {
		return nil, err
	}
	return newClusterFromFileBytes(ctx, b)
}

// NewClusterFromFileBytes initializes a new Cluster from json or yaml bytes.
func NewClusterFromFileBytes(ctx context.Context, configFileBytes []byte) (Cluster, error) {
	return newClusterFromFileBytes(ctx, configFileBytes)
}

// Cluster of shards.
type Cluster interface {
	// Name of the database cluster.
	Name() string

	// AllShards returns a slice of all cluster shards (including writable)
	AllShards() []Shard

	// EveryShard iterates over cluster shards and executes fn on every shard.
	EveryShard(fn func(Shard) error) error

	// ShardByID returns Shard by shard id or nil.
	ShardByID(id uint16) Shard

	// ShardByKey returns Shard by unique key or nil if shard id does not exist.
	ShardByKey(key uint64) Shard

	// ShardsByKeys returns multiple shards with associated keys.
	ShardsByKeys(keys []uint64) map[Shard][]uint64

	// NextShard returns next writable shard using round-robin.
	NextShard() Shard

	// Initialize cluster.
	Initialize() error

	// Close connections to all shards.
	Close()
}

func newCluster(ctx context.Context, cfg ClusterConfig) (*cluster, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	c := &cluster{
		name:     cfg.ClusterName,
		tables:   cfg.Tables,
		all:      make([]Shard, 0, len(cfg.Shards)),
		writable: make([]*shard, 0, len(cfg.Shards)),
		byID:     make(map[uint16]*shard),
	}

	for _, sc := range cfg.Shards {
		conn, err := connect(ctx, sc.DSN)
		if err != nil {
			return nil, err
		}
		s := newShard(sc.ShardID, sc.Writable, conn)
		c.all = append(c.all, s)
		c.byID[s.ID()] = s
		if sc.Writable {
			c.writable = append(c.writable, s)
		}
	}

	var err error
	c.next, err = roundrobin.New[*shard](c.writable...)
	if err != nil {
		return nil, err
	}

	return c, nil
}

func newClusterFromFileBytes(ctx context.Context, b []byte) (Cluster, error) {
	if len(b) == 0 {
		return nil, errors.New("empty config file")
	}
	var (
		cfg ClusterConfig
		err error
	)
	if bytes.HasPrefix(b, []byte("{")) {
		err = json.Unmarshal(b, &cfg)
	} else {
		err = yaml.Unmarshal(b, &cfg)
	}
	if err != nil {
		return nil, err
	}
	return newCluster(ctx, cfg)
}

type cluster struct {
	name     string
	all      []Shard
	writable []*shard
	next     roundrobin.RoundRobin[*shard]
	byID     map[uint16]*shard
	tables   []string
}

func (c *cluster) Name() string {
	return c.name
}

func (c *cluster) AllShards() []Shard {
	return c.all[:]
}

func (c *cluster) EveryShard(fn func(Shard) error) error {
	for _, s := range c.all {
		if err := fn(s); err != nil {
			return err
		}
	}
	return nil
}

func (c *cluster) ShardByID(id uint16) Shard {
	s, ok := c.byID[id]
	if !ok {
		return nil
	}
	return s
}

func (c *cluster) ShardByKey(key uint64) Shard {
	return c.ShardByID(extractShardID(key))
}

func (c *cluster) ShardsByKeys(keys []uint64) map[Shard][]uint64 {
	result := make(map[Shard][]uint64, len(keys))
	for _, key := range keys {
		s := c.ShardByKey(key)
		if s == nil {
			continue
		}
		result[s] = append(result[s], key)
	}
	return result
}

func (c *cluster) NextShard() Shard {
	return c.next.Next()
}

func (c *cluster) Initialize() error {
	for _, s := range c.all {
		if err := s.initialize(c.tables); err != nil {
			return err
		}
	}
	return nil
}

func (c *cluster) Close() {
	for _, s := range c.all {
		s.Close()
	}
}

func extractShardID(id uint64) uint16 {
	shardID := id >> 10
	shardID = shardID & (uint64(1<<10) - 1)
	return uint16(shardID)
}
