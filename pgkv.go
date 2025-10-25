package pgkv

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/jackc/pgx/v5"
	"github.com/xtdlib/pgx/pgxpool"
	"github.com/xtdlib/rat"
)

type KV[K comparable, V any] struct {
	db              *pgxpool.Pool
	tableName       string
	keyNeedsMarshal bool // true if key type is stored as TEXT and needs marshal/unmarshal
}

// Option is a functional option for configuring KV
type Option func(*config)

type config struct {
	dsn       string
	tableName string
}

// DSN sets the PostgreSQL connection string
func DSN(dsn string) Option {
	return func(c *config) {
		c.dsn = dsn
	}
}

// Table sets the table name
func Table(name string) Option {
	return func(c *config) {
		c.tableName = name
	}
}

// TryNew creates a new KV store with required table name and optional configuration.
// Uses PGKV_DSN env var or defaults to localhost postgres if DSN not provided.
func TryNew[K comparable, V any](tableName string, opts ...Option) (*KV[K, V], error) {
	tableName = strings.ReplaceAll(tableName, "-", "_")

	cfg := &config{
		tableName: tableName,
	}

	// Apply options
	for _, opt := range opts {
		opt(cfg)
	}

	// Resolve DSN
	dsn := cfg.dsn
	if dsn == "" && os.Getenv("PGKV_DSN") != "" {
		dsn = os.Getenv("PGKV_DSN")
	}
	if dsn == "" {
		dsn = "postgres://postgres:postgres@localhost:5432/postgres"
	}

	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		return nil, err
	}

	kv := &KV[K, V]{
		db:        pool,
		tableName: cfg.tableName,
	}

	var k K
	keyType := "TEXT"
	kv.keyNeedsMarshal = true // default to true for TEXT type
	switch any(k).(type) {
	case time.Time, *time.Time:
		keyType = "BIGINT"
		kv.keyNeedsMarshal = false
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		keyType = "BIGINT"
		kv.keyNeedsMarshal = false
	case float32, float64:
		keyType = "DOUBLE PRECISION"
		kv.keyNeedsMarshal = false
	}

	query := `
		CREATE TABLE IF NOT EXISTS ` + cfg.tableName + ` (
			key ` + keyType + ` PRIMARY KEY,
			value TEXT NOT NULL
		)
	`

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	_, err = kv.db.Exec(ctx, query)
	if err != nil {
		return nil, err
	}

	return kv, nil
}

// New creates a new KV store with required table name and optional configuration, panics on error.
// Uses PGKV_DSN env var or defaults to localhost postgres if DSN not provided.
func New[K comparable, V any](tableName string, opts ...Option) *KV[K, V] {
	kv, err := TryNew[K, V](tableName, opts...)
	if err != nil {
		panic(err)
	}
	return kv
}

// TrySet stores a key-value pair, returning an error if it fails
func (kv *KV[K, V]) TrySet(key K, value V) (V, error) {
	keyStr, err := marshal(key)
	if err != nil {
		return value, err
	}
	valueStr, err := marshal(value)
	if err != nil {
		return value, err
	}

	query := `
		WITH old AS (
			SELECT value FROM ` + kv.tableName + ` WHERE key = $1
		)
		INSERT INTO ` + kv.tableName + ` (key, value)
		VALUES ($1, $2)
		ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
		RETURNING (SELECT value FROM old) AS old_value
	`
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	var oldValueStr sql.NullString
	err = kv.db.QueryRow(ctx, query, keyStr, valueStr).Scan(&oldValueStr)
	if err != nil {
		return value, err
	}

	var oldValue V
	if oldValueStr.Valid {
		unmarshal(oldValueStr.String, &oldValue)
	}

	// Handle nil pointers before using cmp.Equal (which requires symmetric Equal methods)
	oldValueReflect := reflect.ValueOf(oldValue)
	valueReflect := reflect.ValueOf(value)
	oldIsNil := oldValueReflect.Kind() == reflect.Ptr && oldValueReflect.IsNil()
	valueIsNil := valueReflect.Kind() == reflect.Ptr && valueReflect.IsNil()

	// Only use cmp.Equal if both are non-nil
	changed := false
	if oldIsNil || valueIsNil {
		// If nil states differ, it's a change
		changed = oldIsNil != valueIsNil
	} else {
		// Both non-nil, safe to use cmp.Equal
		changed = !cmp.Equal(value, oldValue)
	}

	if changed {
		slog.Default().Log(context.Background(), slog.LevelDebug, fmt.Sprintf("pgkv: %v: changed", kv.tableName), "key", key, "old", fmt.Sprintf("%v", oldValue), "new", fmt.Sprintf("%v", value))
	}

	return value, nil
}

// Set stores a key-value pair, panics on error, returns the value
func (kv *KV[K, V]) Set(key K, value V) V {
	_, err := kv.TrySet(key, value)
	if err != nil {
		panic(err)
	}
	return value
}

// TryGet retrieves a value by key, returning an error if not found
func (kv *KV[K, V]) TryGet(key K) (V, error) {
	var v V
	keyStr, err := marshal(key)
	if err != nil {
		return v, err
	}

	var valueStr string
	query := `SELECT value FROM ` + kv.tableName + ` WHERE key = $1`
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	err = kv.db.QueryRow(ctx, query, keyStr).Scan(&valueStr)
	if err != nil {
		return v, err
	}

	err = unmarshal(valueStr, &v)
	return v, err
}

// Get retrieves a value by key, panics on error
func (kv *KV[K, V]) Get(key K) V {
	val, err := kv.TryGet(key)
	if err != nil {
		panic(err)
	}
	return val
}

// GetOr retrieves a value by key, returning defaultValue if not found
func (kv *KV[K, V]) GetOr(key K, defaultValue V) V {
	val, err := kv.TryGet(key)
	if err == pgx.ErrNoRows {
		return defaultValue
	}
	if err != nil {
		panic(err)
	}
	return val
}

// TryHas checks if a key exists, returning an error if the check fails
func (kv *KV[K, V]) TryHas(key K) (bool, error) {
	keyStr, err := marshal(key)
	if err != nil {
		return false, err
	}

	var exists int
	query := `SELECT 1 FROM ` + kv.tableName + ` WHERE key = $1 LIMIT 1`
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	err = kv.db.QueryRow(ctx, query, keyStr).Scan(&exists)
	if err == pgx.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// Has checks if a key exists, panics on error
func (kv *KV[K, V]) Has(key K) bool {
	exists, err := kv.TryHas(key)
	if err != nil {
		panic(err)
	}
	return exists
}

// TryDelete removes a key-value pair, returning an error if deletion fails
func (kv *KV[K, V]) TryDelete(key K) error {
	keyStr, err := marshal(key)
	if err != nil {
		return err
	}

	query := `DELETE FROM ` + kv.tableName + ` WHERE key = $1`
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	_, err = kv.db.Exec(ctx, query, keyStr)
	return err
}

// Delete removes a key-value pair, panics on error
func (kv *KV[K, V]) Delete(key K) {
	if err := kv.TryDelete(key); err != nil {
		panic(err)
	}
}

// TryClear removes all key-value pairs, returning an error if it fails
func (kv *KV[K, V]) TryClear() error {
	query := `DELETE FROM ` + kv.tableName
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	_, err := kv.db.Exec(ctx, query)
	return err
}

// Clear removes all key-value pairs, panics on error
func (kv *KV[K, V]) Clear() {
	if err := kv.TryClear(); err != nil {
		panic(err)
	}
}

// TryPurge removes the table, returning an error if it fails
func (kv *KV[K, V]) TryPurge() error {
	query := `DROP TABLE IF EXISTS ` + kv.tableName
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	_, err := kv.db.Exec(ctx, query)
	return err
}

// Purge removes the table, panics on error
func (kv *KV[K, V]) Purge() {
	if err := kv.TryPurge(); err != nil {
		panic(err)
	}
}

// SetNX sets a value only if the key doesn't exist, returns the current value
func (kv *KV[K, V]) SetNX(key K, value V) V {
	if kv.Has(key) {
		return kv.Get(key)
	}
	return kv.Set(key, value)
}

// SetNZ sets a value only if it's not zero, returns the value
func (kv *KV[K, V]) SetNZ(key K, value V) V {
	var zero V
	// if value != zero {
	if !cmp.Equal(value, zero) {
		_, err := kv.TrySet(key, value)
		if err != nil {
			panic(err)
		}
		return value
	}
	// If value is zero, return existing value if it exists
	return kv.GetOr(key, value)
}

func (kv *KV[K, V]) AddRat(key K, delta any) *rat.Rational {
	ratOut, err := kv.TryAddRat(key, delta)
	if err != nil {
		panic(err)
	}
	return ratOut
}

func (kv *KV[K, V]) AddInt(key K, delta int) *rat.Rational {
	ratOut, err := kv.TryAddRat(key, delta)
	if err != nil {
		panic(err)
	}
	return ratOut
}

func (kv *KV[K, V]) TryAddRat(key K, delta any) (*rat.Rational, error) {
	var ratOut *rat.Rational
	out, err := kv.Update(key, func(v V) V {
		old := rat.Rat(v)
		ratOut = old.Add(delta)
		slog.Default().Log(context.Background(), slog.LevelDebug, "pgkv: add", "key", key, "old", old, "new", ratOut, "delta", delta)
		return any(ratOut).(V)
	})
	if err != nil {
		return nil, err
	}
	return rat.Rat(out), nil
}

// Update atomically updates the value at key using the provided function and returns the new value
func (kv *KV[K, V]) Update(key K, fn func(V) V) (V, error) {
	var result V

	keyStr, err := marshal(key)
	if err != nil {
		return result, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	tx, err := kv.db.Begin(ctx)
	if err != nil {
		return result, err
	}
	defer tx.Rollback(ctx)

	// Lock the row for update
	var valueStr string
	query := `SELECT value FROM ` + kv.tableName + ` WHERE key = $1 FOR UPDATE`
	err = tx.QueryRow(ctx, query, keyStr).Scan(&valueStr)

	var current V
	if err == pgx.ErrNoRows {
		// Key doesn't exist, use zero value
		current = result
	} else if err != nil {
		return result, err
	} else {
		err = unmarshal(valueStr, &current)
		if err != nil {
			return result, err
		}
	}

	// Apply function
	result = fn(current)

	// Update
	resultStr, err := marshal(result)
	if err != nil {
		return result, err
	}

	updateQuery := `
		INSERT INTO ` + kv.tableName + ` (key, value)
		VALUES ($1, $2)
		ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
	`
	_, err = tx.Exec(ctx, updateQuery, keyStr, resultStr)
	if err != nil {
		return result, err
	}

	err = tx.Commit(ctx)
	return result, err
}

func marshal(v any) (string, error) {
	switch val := v.(type) {
	case string:
		return val, nil
	case time.Time:
		if val.IsZero() {
			return "0", nil
		}
		return strconv.FormatInt(val.UnixNano(), 10), nil
	case *time.Time:
		if val == nil || val.IsZero() {
			return "0", nil
		}
		return strconv.FormatInt(val.UnixNano(), 10), nil
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return strconv.FormatInt(toInt64(val), 10), nil
	case float32:
		return strconv.FormatFloat(float64(val), 'f', -1, 32), nil
	case float64:
		return strconv.FormatFloat(val, 'f', -1, 64), nil
	default:
		// Check if the type implements fmt.Stringer for text representation
		if stringer, ok := v.(fmt.Stringer); ok {
			return stringer.String(), nil
		}
		b, err := json.Marshal(v)
		return string(b), err
	}
}

func unmarshal(s string, v any) error {
	switch val := v.(type) {
	case *string:
		*val = s
		return nil
	case *time.Time:
		nanos, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return err
		}
		if nanos == 0 {
			*val = time.Time{}
		} else {
			*val = time.Unix(0, nanos)
		}
		return nil
	case **time.Time:
		nanos, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return err
		}
		if nanos == 0 {
			*val = nil
			return nil
		}
		t := time.Unix(0, nanos)
		*val = &t
		return nil
	case *int, *int8, *int16, *int32, *int64, *uint, *uint8, *uint16, *uint32, *uint64:
		i, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return err
		}
		setInt(val, i)
		return nil
	case *float32:
		f, err := strconv.ParseFloat(s, 32)
		if err != nil {
			return err
		}
		*val = float32(f)
		return nil
	case *float64:
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return err
		}
		*val = f
		return nil
	default:
		// Check if v is **T where *T implements sql.Scanner
		rv := reflect.ValueOf(v)
		if rv.Kind() == reflect.Ptr && !rv.IsNil() && rv.Elem().Kind() == reflect.Ptr {
			// v is **T, create new *T instance and scan into it
			elemType := rv.Elem().Type().Elem()
			newElem := reflect.New(elemType)
			if scanner, ok := newElem.Interface().(sql.Scanner); ok {
				if err := scanner.Scan(s); err != nil {
					return err
				}
				rv.Elem().Set(newElem)
				return nil
			}
		}

		// Check if v directly implements sql.Scanner
		if scanner, ok := v.(sql.Scanner); ok {
			return scanner.Scan(s)
		}

		return json.Unmarshal([]byte(s), v)
	}
}

func toInt64(v any) int64 {
	switch val := v.(type) {
	case int:
		return int64(val)
	case int8:
		return int64(val)
	case int16:
		return int64(val)
	case int32:
		return int64(val)
	case int64:
		return val
	case uint:
		return int64(val)
	case uint8:
		return int64(val)
	case uint16:
		return int64(val)
	case uint32:
		return int64(val)
	case uint64:
		return int64(val)
	}
	return 0
}

func setInt(v any, i int64) {
	switch val := v.(type) {
	case *int:
		*val = int(i)
	case *int8:
		*val = int8(i)
	case *int16:
		*val = int16(i)
	case *int32:
		*val = int32(i)
	case *int64:
		*val = i
	case *uint:
		*val = uint(i)
	case *uint8:
		*val = uint8(i)
	case *uint16:
		*val = uint16(i)
	case *uint32:
		*val = uint32(i)
	case *uint64:
		*val = uint64(i)
	}
}

// newScanTarget creates a new scan target for type T.
// If T is a pointer type, it creates a new instance of the pointed-to type.
// For time.Time types, it returns an int64 target and converts on getValue.
// For float types, it returns a float64 target and converts on getValue.
func newScanTarget[T any]() (target any, getValue func() T) {
	var zero T
	rt := reflect.TypeOf(zero)

	// Handle time.Time - scan as int64 (UnixNano)
	if _, ok := any(zero).(time.Time); ok {
		var nanos int64
		return &nanos, func() T {
			t := time.Unix(0, nanos)
			return any(t).(T)
		}
	}

	// Handle float types - scan directly as float64
	if _, ok := any(zero).(float32); ok {
		var f float64
		return &f, func() T {
			return any(float32(f)).(T)
		}
	}
	if _, ok := any(zero).(float64); ok {
		var f float64
		return &f, func() T {
			return any(f).(T)
		}
	}

	if rt != nil && rt.Kind() == reflect.Ptr {
		// Check if it's *time.Time
		if rt.Elem() == reflect.TypeOf(time.Time{}) {
			var nanos int64
			return &nanos, func() T {
				if nanos == 0 {
					return zero // nil pointer
				}
				t := time.Unix(0, nanos)
				return any(&t).(T)
			}
		}

		// Other pointer types
		elem := reflect.New(rt.Elem())
		return elem.Interface(), func() T {
			return elem.Interface().(T)
		}
	}

	// T is not a pointer, use standard approach
	target = new(T)
	return target, func() T {
		return *target.(*T)
	}
}

// Keys iterates over all keys in forward order
func (kv *KV[K, V]) Keys(yield func(K) bool) {
	query := `SELECT key FROM ` + kv.tableName + ` ORDER BY key`
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()
	rows, err := kv.db.Query(ctx, query)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	keys, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (K, error) {
		if kv.keyNeedsMarshal {
			var keyStr string
			if err := row.Scan(&keyStr); err != nil {
				var zero K
				return zero, err
			}
			target, getValue := newScanTarget[K]()
			if err := unmarshal(keyStr, target); err != nil {
				var zero K
				return zero, err
			}
			return getValue(), nil
		}
		// Native type key (time.Time or integer) scanned directly from PostgreSQL
		target, getValue := newScanTarget[K]()
		err := row.Scan(target)
		return getValue(), err
	})
	if err != nil {
		panic(err)
	}

	for _, k := range keys {
		if !yield(k) {
			return
		}
	}
}

// KeysBackward iterates over all keys in reverse order
func (kv *KV[K, V]) KeysBackward(yield func(K) bool) {
	query := `SELECT key FROM ` + kv.tableName + ` ORDER BY key DESC`
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()
	rows, err := kv.db.Query(ctx, query)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	keys, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (K, error) {
		if kv.keyNeedsMarshal {
			var keyStr string
			if err := row.Scan(&keyStr); err != nil {
				var zero K
				return zero, err
			}
			target, getValue := newScanTarget[K]()
			if err := unmarshal(keyStr, target); err != nil {
				var zero K
				return zero, err
			}
			return getValue(), nil
		}
		// Native type key (time.Time or integer) scanned directly from PostgreSQL
		target, getValue := newScanTarget[K]()
		err := row.Scan(target)
		return getValue(), err
	})
	if err != nil {
		panic(err)
	}

	for _, k := range keys {
		if !yield(k) {
			return
		}
	}
}

// All iterates over all key-value pairs in forward order
func (kv *KV[K, V]) All(yield func(K, V) bool) {
	query := `SELECT key, value FROM ` + kv.tableName + ` ORDER BY key`
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()
	rows, err := kv.db.Query(ctx, query)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	type kvPair struct {
		key   K
		value V
	}

	pairs, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (kvPair, error) {
		var key K
		var valueStr string

		if kv.keyNeedsMarshal {
			var keyStr string
			if err := row.Scan(&keyStr, &valueStr); err != nil {
				return kvPair{}, err
			}
			keyTarget, getKey := newScanTarget[K]()
			if err := unmarshal(keyStr, keyTarget); err != nil {
				return kvPair{}, err
			}
			key = getKey()
		} else {
			// Native type key (time.Time or integer) scanned directly from PostgreSQL
			keyTarget, getKey := newScanTarget[K]()
			if err := row.Scan(keyTarget, &valueStr); err != nil {
				return kvPair{}, err
			}
			key = getKey()
		}

		valTarget, getVal := newScanTarget[V]()
		if err := unmarshal(valueStr, valTarget); err != nil {
			return kvPair{}, err
		}

		return kvPair{key: key, value: getVal()}, nil
	})
	if err != nil {
		panic(err)
	}

	for _, pair := range pairs {
		if !yield(pair.key, pair.value) {
			return
		}
	}
}

// KeysWhere returns an iterator over keys matching the WHERE clause in forward order
func (kv *KV[K, V]) KeysWhere(whereClause string) func(yield func(K) bool) {
	return func(yield func(K) bool) {
		query := `SELECT key FROM ` + kv.tableName + ` WHERE ` + whereClause + ` ORDER BY key`
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
		defer cancel()
		rows, err := kv.db.Query(ctx, query)
		if err != nil {
			panic(err)
		}
		defer rows.Close()

		keys, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (K, error) {
			if kv.keyNeedsMarshal {
				var keyStr string
				if err := row.Scan(&keyStr); err != nil {
					var zero K
					return zero, err
				}
				target, getValue := newScanTarget[K]()
				if err := unmarshal(keyStr, target); err != nil {
					var zero K
					return zero, err
				}
				return getValue(), nil
			}
			// Native type key (time.Time or integer) scanned directly from PostgreSQL
			target, getValue := newScanTarget[K]()
			err := row.Scan(target)
			return getValue(), err
		})
		if err != nil {
			panic(err)
		}

		for _, k := range keys {
			if !yield(k) {
				return
			}
		}
	}
}

// AllWhere returns an iterator over key-value pairs matching the WHERE clause in forward order
func (kv *KV[K, V]) AllWhere(whereClause string) func(yield func(K, V) bool) {
	return func(yield func(K, V) bool) {
		query := `SELECT key, value FROM ` + kv.tableName + ` WHERE ` + whereClause + ` ORDER BY key`
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
		defer cancel()
		rows, err := kv.db.Query(ctx, query)
		if err != nil {
			panic(err)
		}
		defer rows.Close()

		type kvPair struct {
			key   K
			value V
		}

		pairs, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (kvPair, error) {
			var key K
			var valueStr string

			if kv.keyNeedsMarshal {
				var keyStr string
				if err := row.Scan(&keyStr, &valueStr); err != nil {
					return kvPair{}, err
				}
				keyTarget, getKey := newScanTarget[K]()
				if err := unmarshal(keyStr, keyTarget); err != nil {
					return kvPair{}, err
				}
				key = getKey()
			} else {
				// Native type key (time.Time or integer) scanned directly from PostgreSQL
				keyTarget, getKey := newScanTarget[K]()
				if err := row.Scan(keyTarget, &valueStr); err != nil {
					return kvPair{}, err
				}
				key = getKey()
			}

			valTarget, getVal := newScanTarget[V]()
			if err := unmarshal(valueStr, valTarget); err != nil {
				return kvPair{}, err
			}

			return kvPair{key: key, value: getVal()}, nil
		})
		if err != nil {
			panic(err)
		}

		for _, pair := range pairs {
			if !yield(pair.key, pair.value) {
				return
			}
		}
	}
}
