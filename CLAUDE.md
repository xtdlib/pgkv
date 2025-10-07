# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

`pgkv` is a PostgreSQL-backed generic key-value store for Go using generics. It provides a simple KV[K, V] interface where both keys and values are type-safe.

## Core Architecture

### Type System & Database Mapping

The library uses Go generics (K comparable, V comparable) and automatically maps types to PostgreSQL column types:

- **Keys (K)**:
  - `time.Time` → `TIMESTAMP` column type (scanned directly by pgx)
  - Integer types (`int`, `int8`, `int16`, `int32`, `int64`, `uint`, `uint8`, `uint16`, `uint32`, `uint64`) → `BIGINT` column type (scanned directly by pgx)
  - Other types → `TEXT` column type (JSON marshaled)

- **Values (V)**: Always stored as `TEXT` (marshaled appropriately based on type)

Type detection happens in `New()` at pgkv.go:48-56 where it inspects the zero value of K to determine the correct PostgreSQL column type.

### Marshal/Unmarshal Logic

Located in pgkv.go:310-346. The marshal/unmarshal functions handle type conversion for **values only**:
- `string`: stored as-is
- `time.Time`: formatted as "2006-01-02 15:04:05.999999" (though typically handled directly by pgx)
- Integer types: converted to/from string representation
- Other types: JSON marshaled/unmarshaled

**Important**: Keys with native PostgreSQL types (`time.Time`, integers) are scanned directly by pgx in iterators (Keys, KeysBackward, All), not through marshal/unmarshal. Only TEXT-based keys use marshal/unmarshal.

When adding support for new key types, update:
1. Type detection in `New()` (pgkv.go:48-56)
2. Marshal/unmarshal functions for value handling
3. Consider if pgx can scan the type directly (like TIMESTAMP/BIGINT)

### Connection Management

The library accepts either a DSN string or a pre-configured DB interface (pgx.Pool/pgx.Tx). DSN resolution order:
1. Provided DSN parameter
2. `PGKV_DSN` environment variable
3. Default: `postgres://postgres:postgres@localhost:5432/postgres`

### Atomic Operations

`Update()` (pgkv.go:246-302) provides transaction-based atomic updates using SELECT FOR UPDATE with row-level locking. This is used by `AddRat()` for concurrent rational number arithmetic.

## Development Commands

Run tests:
```bash
go test
```

Run examples:
```bash
cd example/time && go run main.go
cd example/addrat && go run main.go
```

## Design Principles

- Simple, short code - avoid overcomplication
- Each method has both a `Try*` version (returns error) and a panic version
- Iterator functions (Keys, KeysBackward, All) use Go 1.23+ range-over-func pattern
- All database operations use 3-second timeouts
