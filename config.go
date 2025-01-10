package database

import (
	"errors"
	"fmt"
	"net/url"
)

// ClusterConfig struct.
type ClusterConfig struct {
	ClusterName string        `yaml:"cluster_name" json:"cluster_name"`
	Shards      []ShardConfig `yaml:"shards" json:"shards"`
	Tables      []string      `yaml:"tables" json:"tables"`
}

type ShardConfig struct {
	ShardID  uint16 `yaml:"shard_id" json:"shard_id"`
	DSN      string `yaml:"dsn" json:"dsn"`
	Writable bool   `yaml:"writable" json:"writable"`
}

func (cc ClusterConfig) validate() error {
	if cc.ClusterName == "" {
		return errors.New("cluster_name is required")
	}
	if len(cc.Shards) == 0 {
		return errors.New("shards are required")
	}
	if len(cc.Tables) == 0 {
		return errors.New("no sharded tables defined for cluster")
	}
	dup := make(map[uint16]struct{}, len(cc.Shards))
	writable := false
	for _, s := range cc.Shards {
		if err := s.validate(); err != nil {
			return err
		}
		if _, ok := dup[s.ShardID]; ok {
			return fmt.Errorf("duplicate shard_id: %d", s.ShardID)
		}
		if s.Writable {
			writable = true
		}
		dup[s.ShardID] = struct{}{}
	}
	if !writable {
		return fmt.Errorf("there should be at least one writable shard")
	}
	return nil
}

func (sc ShardConfig) validate() error {
	if sc.ShardID == 0 {
		return errors.New("shard_id cannot be zero")
	}
	_, err := url.Parse(sc.DSN)
	if err != nil {
		return fmt.Errorf("invalid dsn: %s", sc.DSN)
	}
	return nil
}
