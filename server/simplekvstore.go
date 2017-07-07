package server

import (
	"sync"
	"sync/atomic"
	"time"
)

// SimpleValue structure for the k/v storage. Not optimized for space saving. No LRU.
type SimpleValue struct {
	RawData []byte
	Flag    uint32
	CAS     uint64
	TTL     int
}

// Simple storage for all k/v pairs. Uses a RWMutex for concurrency control.
var simplekvMap = map[string]SimpleValue{}
var simplekvMutex sync.RWMutex

// GetFromSimpleKV looks up a key with locking.
func GetFromSimpleKV(key string) (SimpleValue, bool) {
	simplekvMutex.RLock()
	val, ok := simplekvMap[key]
	if !ok {
		simplekvMutex.RUnlock()
		return SimpleValue{}, false
	}
	cas := val.CAS
	simplekvMutex.RUnlock()
	if val.TTL != 0 && val.TTL < time.Now().Second() {
		simplekvMutex.Lock()
		val, ok = simplekvMap[key]
		if ok && val.CAS == cas {
			delete(simplekvMap, key)
			ok = false
		}
		simplekvMutex.Unlock()
		if !ok {
			return SimpleValue{}, false
		}
	}
	return val, true
}

// AddToSimpleKV will only set a value only when it does not exist yet. Lock is being held during update. CAS value will be bumped.
func AddToSimpleKV(key string, newVal SimpleValue) (SimpleValue, bool) {
	simplekvMutex.Lock()
	defer simplekvMutex.Unlock()
	_, ok := simplekvMap[key]
	if ok {
		// Already exists is a failure case
		return newVal, false
	}
	newVal.CAS = atomic.AddUint64(&casID, 1)
	if newVal.CAS == 0 {
		// skip value 0 for CAS value
		newVal.CAS = atomic.AddUint64(&casID, 1)
	}
	simplekvMap[key] = newVal
	return newVal, true
}

// SetToSimpleKV handles normal set and replace. Replace will fail is a key does not exist. For an existing key, both set and replace will check CAS if it's not 0.
// Return values are 1. set value, 2. is key missing, 3. is successful.
func SetToSimpleKV(key string, newVal SimpleValue, cas uint64, replace bool) (SimpleValue, bool, bool) {
	simplekvMutex.Lock()
	defer simplekvMutex.Unlock()
	oldVal, ok := simplekvMap[key]
	if !ok && replace {
		// Replace key not found
		return newVal, true, false
	}
	if ok && cas != 0 && cas != oldVal.CAS {
		// CAS does not match
		return newVal, false, false
	}
	newVal.CAS = atomic.AddUint64(&casID, 1)
	if newVal.CAS == 0 {
		// skip value 0 for CAS value
		newVal.CAS = atomic.AddUint64(&casID, 1)
	}
	simplekvMap[key] = newVal
	return newVal, false, true
}
