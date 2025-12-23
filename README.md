# silver-storage

ParityDB-based persistent storage for SilverBitcoin 512-bit blockchain.

## Overview

`silver-storage` provides a high-performance, persistent storage layer for the SilverBitcoin blockchain. It wraps ParityDB with a comprehensive abstraction layer for storing blocks, transactions, objects, mining data, events, and tokens.

## Key Components

### 1. Database Abstraction (`db.rs`)
- Database abstraction layer
- Connection pooling
- Transaction management
- Error handling
- Async/await support
- Thread-safe operations

### 2. Block Store (`block_store.rs`)
- Block storage and retrieval
- Block indexing by height and hash
- Block validation
- Block serialization/deserialization
- Efficient block queries
- Block history tracking

### 3. Transaction Store (`transaction_store.rs`)
- Transaction storage and retrieval
- Transaction indexing by hash
- Transaction status tracking
- Mempool management
- Transaction history
- Efficient transaction queries

### 4. Object Store (`object_store.rs`)
- Object storage and retrieval
- Object indexing
- Object versioning
- Object serialization/deserialization
- Efficient object queries
- Object lifecycle management

### 5. Mining Store (`mining_store.rs`)
- Mining data storage
- Work package storage
- Share tracking
- Miner statistics
- Reward tracking
- Mining history

### 6. Event Store (`event_store.rs`)
- Event storage and retrieval
- Event indexing
- Event filtering
- Event history
- Event replay capability
- Efficient event queries

### 7. Token Store (`token_store.rs`)
- Token storage and retrieval
- Token metadata
- Token balance tracking
- Token transfer history
- Token supply management
- Efficient token queries

## Features

- **ParityDB Backend**: High-performance key-value storage
- **Object-Centric Model**: Assets as first-class objects
- **Persistent Storage**: Block store, transaction store, object store, mining store
- **Archive Chain**: Complete historical record for auditing
- **Compression**: LZ4 compression for efficient storage
- **Async/Await**: Full tokio integration for non-blocking operations
- **Thread-Safe**: Arc, RwLock, DashMap for safe concurrent access
- **Error Handling**: Comprehensive error types and propagation
- **No Unsafe Code**: 100% safe Rust
- **Efficient Queries**: Indexed storage for fast lookups

## Dependencies

- **Core**: silver-core
- **Storage**: parity-db, lz4
- **Async Runtime**: tokio, async-trait
- **Serialization**: serde, serde_json
- **Concurrency**: parking_lot, dashmap
- **Utilities**: hex, bytes, chrono, num_cpus, sysinfo

## Usage

```rust
use silver_storage::{
    db::Database,
    block_store::BlockStore,
    transaction_store::TransactionStore,
    object_store::ObjectStore,
    mining_store::MiningStore,
};

// Create database
let db = Database::new("./data")?;

// Create stores
let block_store = BlockStore::new(db.clone())?;
let tx_store = TransactionStore::new(db.clone())?;
let object_store = ObjectStore::new(db.clone())?;
let mining_store = MiningStore::new(db.clone())?;

// Store a block
block_store.store_block(block)?;

// Retrieve a block
let block = block_store.get_block_by_hash(&hash)?;

// Store a transaction
tx_store.store_transaction(tx)?;

// Retrieve a transaction
let tx = tx_store.get_transaction(&tx_hash)?;

// Store an object
object_store.store_object(object)?;

// Retrieve an object
let obj = object_store.get_object(&object_id)?;

// Store mining data
mining_store.store_work_package(work)?;

// Retrieve mining data
let work = mining_store.get_work_package(&work_id)?;
```

## Testing

```bash
# Run all tests
cargo test -p silver-storage

# Run with output
cargo test -p silver-storage -- --nocapture

# Run specific test
cargo test -p silver-storage block_store_operations

# Run benchmarks
cargo bench -p silver-storage
```

## Code Quality

```bash
# Run clippy
cargo clippy -p silver-storage --release

# Check formatting
cargo fmt -p silver-storage --check

# Format code
cargo fmt -p silver-storage
```

## Architecture

```
silver-storage/
├── src/
│   ├── db.rs                   # Database abstraction
│   ├── block_store.rs          # Block storage
│   ├── transaction_store.rs    # Transaction storage
│   ├── object_store.rs         # Object storage
│   ├── mining_store.rs         # Mining data storage
│   ├── event_store.rs          # Event storage
│   ├── token_store.rs          # Token storage
│   └── lib.rs                  # Storage exports
├── Cargo.toml
└── README.md
```

## Storage Layout

```
./data/
├── blocks/                     # Block storage
│   ├── by_hash/               # Blocks indexed by hash
│   ├── by_height/             # Blocks indexed by height
│   └── metadata/              # Block metadata
├── transactions/              # Transaction storage
│   ├── by_hash/               # Transactions indexed by hash
│   ├── by_address/            # Transactions indexed by address
│   └── mempool/               # Mempool transactions
├── objects/                   # Object storage
│   ├── by_id/                 # Objects indexed by ID
│   ├── by_type/               # Objects indexed by type
│   └── versions/              # Object versions
├── mining/                    # Mining data storage
│   ├── work_packages/         # Work packages
│   ├── shares/                # Share tracking
│   └── statistics/            # Mining statistics
├── events/                    # Event storage
│   ├── by_type/               # Events indexed by type
│   └── by_timestamp/          # Events indexed by timestamp
└── tokens/                    # Token storage
    ├── by_id/                 # Tokens indexed by ID
    ├── balances/              # Token balances
    └── transfers/             # Transfer history
```

## Performance

- **Block Storage**: ~1ms per block
- **Block Retrieval**: ~100µs per block
- **Transaction Storage**: ~100µs per transaction
- **Transaction Retrieval**: ~10µs per transaction
- **Object Storage**: ~100µs per object
- **Object Retrieval**: ~10µs per object
- **Compression Ratio**: ~50% (LZ4)
- **Query Performance**: ~1µs per indexed lookup

## Scalability

- **Concurrent Operations**: Full async/await support
- **Thread-Safe**: Safe concurrent access with RwLock
- **Efficient Indexing**: Fast lookups with indexed storage
- **Compression**: Reduces storage requirements by ~50%
- **Archival**: Complete historical record for auditing

## Security Considerations

- **Data Integrity**: Checksums for data validation
- **Atomic Operations**: Transaction support for consistency
- **Access Control**: Permission-based access (future)
- **Encryption**: Optional encryption at rest (future)
- **No Unsafe Code**: 100% safe Rust

## Backup and Recovery

- **Snapshots**: Point-in-time snapshots for backup
- **Incremental Backup**: Efficient incremental backups
- **Recovery**: Full recovery from backups
- **Verification**: Data integrity verification

## License

Apache License 2.0 - See LICENSE file for details

## Contributing

Contributions are welcome! Please ensure:
1. All tests pass (`cargo test -p silver-storage`)
2. Code is formatted (`cargo fmt -p silver-storage`)
3. No clippy warnings (`cargo clippy -p silver-storage --release`)
4. Documentation is updated
5. Performance implications are considered
