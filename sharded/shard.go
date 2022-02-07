package sharded

import (
	"errors"
	"fmt"
	"hash"
	"hash/fnv"
	"sync"
)

// Sharder returns a shard for the specified key and number of shards in the
// log. The number of shards specified is expected to match the number of shards
// during log creation to avoid undefined behavior.
type Sharder interface {
	Shard(key []byte, shards uint) (uint, error)
}

type defaultSharder struct {
	sync.Mutex
	hash32 hash.Hash32
}

func newDefaultSharder() *defaultSharder {
	return &defaultSharder{
		hash32: fnv.New32a(),
	}
}

func (d *defaultSharder) Shard(key []byte, shards uint) (uint, error) {
	h, err := d.hash(key)
	if err != nil {
		return 0, fmt.Errorf("hash key: %w", err)
	}

	shard := int32(h) % int32(shards)
	if shard < 0 {
		shard = -shard
	}
	return uint(shard), nil
}

func (d *defaultSharder) hash(key []byte) (uint32, error) {
	d.Lock()
	defer d.Unlock()

	d.hash32.Reset()
	_, err := d.hash32.Write(key)
	if err != nil {
		return 0, err
	}

	return d.hash32.Sum32(), nil
}

// KeySharder assigns a shard per unique key
type KeySharder struct {
	mu     sync.RWMutex
	shards map[string]uint
}

// NewKeySharder creates a new key-based Sharder, assigning a shard to each
// unique key. The caller must ensure that there are at least len(keys) shards
// available in the log.
func NewKeySharder(keys []string) *KeySharder {
	ks := KeySharder{shards: map[string]uint{}}
	for shard, key := range keys {
		ks.shards[key] = uint(shard)
	}

	return &ks
}

// Shard implements Sharder interface
func (k *KeySharder) Shard(key []byte, shards uint) (uint, error) {
	k.mu.RLock()
	defer k.mu.RUnlock()

	if len(k.shards) > int(shards) {
		return 0, fmt.Errorf("number of keys greater than available shards")
	}

	if s, ok := k.shards[string(key)]; ok {
		return s, nil
	}

	return 0, errors.New("shard not found")
}
