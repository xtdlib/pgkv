package pgkv

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/xtdlib/pgx/pgxpool"
	"github.com/xtdlib/rat"
)

type DB interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Begin(ctx context.Context) (pgx.Tx, error)
}

type KV[K comparable, V comparable] struct {
	db        DB
	tableName string
}

// TryNew creates a new KV store with the given table name and initializes the table
func TryNew[K comparable, V comparable](dsn string, tableName string) (*KV[K, V], error) {
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
		tableName: tableName,
	}

	var k K
	keyType := "TEXT"
	if _, ok := any(k).(time.Time); ok {
		keyType = "TIMESTAMP"
	}
	switch any(k).(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		keyType = "BIGINT"
	}

	query := `
		CREATE TABLE IF NOT EXISTS ` + tableName + ` (
			key ` + keyType + ` PRIMARY KEY,
			value TEXT NOT NULL
		)
	`

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	_, err = kv.db.Exec(ctx, query)
	if err != nil {
		return nil, err
	}

	return kv, nil
}

// New creates a new KV store with the given table name and initializes the table, panics on error
func New[K comparable, V comparable](dsn string, tableName string) *KV[K, V] {
	kv, err := TryNew[K, V](dsn, tableName)
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
		INSERT INTO ` + kv.tableName + ` (key, value)
		VALUES ($1, $2)
		ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
	`
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	_, err = kv.db.Exec(ctx, query, keyStr, valueStr)
	return value, err
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
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
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
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
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
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
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
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
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
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
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
	if value != zero {
		_, err := kv.TrySet(key, value)
		if err != nil {
			panic(err)
		}
	}
	return value
}

func (kv *KV[K, V]) AddRat(key K, delta any) *rat.Rational {
	ratOut, err := kv.TryAddRat(key, delta)
	if err != nil {
		panic(err)
	}
	return ratOut
}

func (kv *KV[K, V]) TryAddRat(key K, delta any) (*rat.Rational, error) {
	var ratOut *rat.Rational
	out, err := kv.Update(key, func(v V) V {
		ratOut = rat.Rat(v).Add(delta)
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

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
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
		return val.Format("2006-01-02 15:04:05.999999"), nil
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return strconv.FormatInt(toInt64(val), 10), nil
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
		t, err := time.Parse("2006-01-02 15:04:05.999999", s)
		if err != nil {
			return err
		}
		*val = t
		return nil
	case *int, *int8, *int16, *int32, *int64, *uint, *uint8, *uint16, *uint32, *uint64:
		i, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return err
		}
		setInt(val, i)
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
func newScanTarget[T any]() (target any, getValue func() T) {
	var zero T
	rt := reflect.TypeOf(zero)

	if rt != nil && rt.Kind() == reflect.Ptr {
		// T is a pointer type, create new instance of the element type
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
		keyTarget, getKey := newScanTarget[K]()
		var valueStr string
		if err := row.Scan(keyTarget, &valueStr); err != nil {
			return kvPair{}, err
		}

		valTarget, getVal := newScanTarget[V]()
		if err := unmarshal(valueStr, valTarget); err != nil {
			return kvPair{}, err
		}

		return kvPair{key: getKey(), value: getVal()}, nil
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
