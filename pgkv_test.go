package pgkv

import (
	"testing"
)

func TestKeys(t *testing.T) {
	kv, err := New[string, string]("", "test_keys")
	if err != nil {
		t.Fatalf("Failed to create KV: %v", err)
	}

	kv.Clear()

	// Test empty store
	var keys []string
	kv.Keys(func(key string) bool {
		keys = append(keys, key)
		return true
	})
	if len(keys) != 0 {
		t.Fatalf("Expected 0 keys, got %d", len(keys))
	}

	// Add test data
	kv.Set("zebra", "value1")
	kv.Set("apple", "value2")
	kv.Set("banana", "value3")

	// Test forward iteration - should be sorted
	keys = nil
	kv.Keys(func(key string) bool {
		keys = append(keys, key)
		return true
	})

	expected := []string{"apple", "banana", "zebra"}
	if len(keys) != len(expected) {
		t.Fatalf("Expected %d keys, got %d", len(expected), len(keys))
	}
	for i, key := range keys {
		if key != expected[i] {
			t.Fatalf("At index %d: expected %s, got %s", i, expected[i], key)
		}
	}

	// Test early termination
	keys = nil
	kv.Keys(func(key string) bool {
		keys = append(keys, key)
		return len(keys) < 2
	})
	if len(keys) != 2 {
		t.Fatalf("Expected 2 keys with early termination, got %d", len(keys))
	}
	if keys[0] != "apple" || keys[1] != "banana" {
		t.Fatalf("Early termination keys incorrect: got %v", keys)
	}
}

func TestKeysBackward(t *testing.T) {
	kv, err := New[string, string]("", "test_keys_backward")
	if err != nil {
		t.Fatalf("Failed to create KV: %v", err)
	}

	kv.Clear()

	// Test empty store
	var keys []string
	kv.KeysBackward(func(key string) bool {
		keys = append(keys, key)
		return true
	})
	if len(keys) != 0 {
		t.Fatalf("Expected 0 keys, got %d", len(keys))
	}

	// Add test data
	kv.Set("zebra", "value1")
	kv.Set("apple", "value2")
	kv.Set("banana", "value3")

	// Test backward iteration - should be reverse sorted
	keys = nil
	kv.KeysBackward(func(key string) bool {
		keys = append(keys, key)
		return true
	})

	expected := []string{"zebra", "banana", "apple"}
	if len(keys) != len(expected) {
		t.Fatalf("Expected %d keys, got %d", len(expected), len(keys))
	}
	for i, key := range keys {
		if key != expected[i] {
			t.Fatalf("At index %d: expected %s, got %s", i, expected[i], key)
		}
	}

	// Test early termination
	keys = nil
	kv.KeysBackward(func(key string) bool {
		keys = append(keys, key)
		return len(keys) < 2
	})
	if len(keys) != 2 {
		t.Fatalf("Expected 2 keys with early termination, got %d", len(keys))
	}
	if keys[0] != "zebra" || keys[1] != "banana" {
		t.Fatalf("Early termination keys incorrect: got %v", keys)
	}
}

func TestHas(t *testing.T) {
	kv, err := New[string, int]("", "test_has")
	if err != nil {
		t.Fatalf("Failed to create KV: %v", err)
	}

	kv.Clear()

	// Test non-existent key
	if kv.Has("missing") {
		t.Fatal("Expected Has to return false for missing key")
	}

	// Test existing key
	kv.Set("exists", 42)
	if !kv.Has("exists") {
		t.Fatal("Expected Has to return true for existing key")
	}
}

func TestDelete(t *testing.T) {
	kv, err := New[string, int]("", "test_delete")
	if err != nil {
		t.Fatalf("Failed to create KV: %v", err)
	}

	kv.Clear()

	kv.Set("key1", 100)
	if !kv.Has("key1") {
		t.Fatal("Key should exist after Set")
	}

	kv.Delete("key1")
	if kv.Has("key1") {
		t.Fatal("Key should not exist after Delete")
	}
}

func TestClear(t *testing.T) {
	kv, err := New[string, string]("", "test_clear")
	if err != nil {
		t.Fatalf("Failed to create KV: %v", err)
	}

	kv.Set("key1", "val1")
	kv.Set("key2", "val2")
	kv.Set("key3", "val3")

	kv.Clear()

	var count int
	kv.Keys(func(key string) bool {
		count++
		return true
	})

	if count != 0 {
		t.Fatalf("Expected 0 keys after Clear, got %d", count)
	}
}

func TestGetOr(t *testing.T) {
	kv, err := New[string, int]("", "test_getor")
	if err != nil {
		t.Fatalf("Failed to create KV: %v", err)
	}

	kv.Clear()

	// Test missing key returns default
	val := kv.GetOr("missing", 999)
	if val != 999 {
		t.Fatalf("Expected default value 999, got %d", val)
	}

	// Test existing key returns actual value
	kv.Set("exists", 42)
	val = kv.GetOr("exists", 999)
	if val != 42 {
		t.Fatalf("Expected actual value 42, got %d", val)
	}
}

func TestSetNX(t *testing.T) {
	kv, err := New[string, int]("", "test_setnx")
	if err != nil {
		t.Fatalf("Failed to create KV: %v", err)
	}

	kv.Clear()

	// First SetNX should set the value
	val := kv.SetNX("key1", 100)
	if val != 100 {
		t.Fatalf("Expected 100, got %d", val)
	}

	// Second SetNX should return existing value
	val = kv.SetNX("key1", 200)
	if val != 100 {
		t.Fatalf("Expected existing value 100, got %d", val)
	}
}

func TestSetNZ(t *testing.T) {
	kv, err := New[string, int]("", "test_setnz")
	if err != nil {
		t.Fatalf("Failed to create KV: %v", err)
	}

	kv.Clear()

	// SetNZ with non-zero should set
	val := kv.SetNZ("key1", 100)
	if val != 100 {
		t.Fatalf("Expected 100, got %d", val)
	}
	if !kv.Has("key1") {
		t.Fatal("Key should exist after SetNZ with non-zero value")
	}

	// SetNZ with zero should not set
	kv.SetNZ("key2", 0)
	if kv.Has("key2") {
		t.Fatal("Key should not exist after SetNZ with zero value")
	}
}

func TestTryGet(t *testing.T) {
	kv, err := New[string, int]("", "test_tryget")
	if err != nil {
		t.Fatalf("Failed to create KV: %v", err)
	}

	kv.Clear()

	// Try missing key
	_, err = kv.TryGet("missing")
	if err == nil {
		t.Fatal("Expected error for missing key")
	}

	// Try existing key
	kv.Set("exists", 42)
	val, err := kv.TryGet("exists")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if val != 42 {
		t.Fatalf("Expected 42, got %d", val)
	}
}

func TestTryHas(t *testing.T) {
	kv, err := New[string, int]("", "test_tryhas")
	if err != nil {
		t.Fatalf("Failed to create KV: %v", err)
	}

	kv.Clear()

	// Check missing key
	exists, err := kv.TryHas("missing")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if exists {
		t.Fatal("Expected false for missing key")
	}

	// Check existing key
	kv.Set("exists", 42)
	exists, err = kv.TryHas("exists")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if !exists {
		t.Fatal("Expected true for existing key")
	}
}
