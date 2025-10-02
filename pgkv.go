package pgkv

import (
	"context"
	"database/sql"
	"encoding/json"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
)

type KV[K comparable, V any] struct {
	db        *pgx.Conn
	tableName string
}

// New creates a new KV store with the given table name and initializes the table
func New[K comparable, V any](db *pgx.Conn, tableName string) (*KV[K, V], error) {
	kv := &KV[K, V]{
		db:        db,
		tableName: tableName,
	}

	query := `
		CREATE TABLE IF NOT EXISTS ` + tableName + ` (
			key TEXT PRIMARY KEY,
			value TEXT NOT NULL
		)
	`

	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, time.Second*3)
	defer cancel()
	_, err := db.Exec(ctx, query)
	if err != nil {
		return nil, err
	}

	return kv, nil
}

// Set stores a key-value pair
func (kv *KV[K, V]) Set(ctx context.Context, key K, value V) error {
	keyStr, err := marshal(key)
	if err != nil {
		return err
	}
	valueStr, err := marshal(value)
	if err != nil {
		return err
	}

	query := `
		INSERT INTO ` + kv.tableName + ` (key, value)
		VALUES ($1, $2)
		ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
	`
	_, err = kv.db.Exec(ctx, query, keyStr, valueStr)
	return err
}

// Get retrieves a value by key
func (kv *KV[K, V]) Get(ctx context.Context, key K) (V, error) {
	var v V
	keyStr, err := marshal(key)
	if err != nil {
		return v, err
	}

	var valueStr string
	query := `SELECT value FROM ` + kv.tableName + ` WHERE key = $1`
	err = kv.db.QueryRow(ctx, query, keyStr).Scan(&valueStr)
	if err != nil {
		return v, err
	}

	err = unmarshal(valueStr, &v)
	return v, err
}

// Add atomically adds delta to the numeric value at key and returns the new value
func (kv *KV[K, V]) Add(ctx context.Context, key K, delta int64) (int64, error) {
	keyStr, err := marshal(key)
	if err != nil {
		return 0, err
	}

	query := `
		INSERT INTO ` + kv.tableName + ` (key, value)
		VALUES ($1, $2)
		ON CONFLICT (key) DO UPDATE
		SET value = (CAST(` + kv.tableName + `.value AS BIGINT) + $2)::TEXT
		RETURNING CAST(value AS BIGINT)
	`
	var newValue int64
	err = kv.db.QueryRow(ctx, query, keyStr, strconv.FormatInt(delta, 10)).Scan(&newValue)
	if err != nil {
		return 0, err
	}
	return newValue, nil
}

// Update atomically updates the value at key using the provided function and returns the new value
func (kv *KV[K, V]) Update(ctx context.Context, key K, fn func(V) V) (V, error) {
	var result V

	keyStr, err := marshal(key)
	if err != nil {
		return result, err
	}

	tx, err := kv.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return result, err
	}
	defer tx.Rollback(ctx)

	// Lock the row for update
	var valueStr string
	query := `SELECT value FROM ` + kv.tableName + ` WHERE key = $1 FOR UPDATE`
	err = tx.QueryRow(ctx, query, keyStr).Scan(&valueStr)

	var current V
	if err == sql.ErrNoRows {
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
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return strconv.FormatInt(toInt64(val), 10), nil
	default:
		b, err := json.Marshal(v)
		return string(b), err
	}
}

func unmarshal(s string, v any) error {
	switch val := v.(type) {
	case *string:
		*val = s
		return nil
	case *int, *int8, *int16, *int32, *int64, *uint, *uint8, *uint16, *uint32, *uint64:
		i, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return err
		}
		setInt(val, i)
		return nil
	default:
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
