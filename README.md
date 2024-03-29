# Shackle

Shackle is a horizontally scalable distributed hash set with automatic eviction, linearizable isolation
and two phase commit built on LMDB and Dragonboat. It was created specifically to provide a durable,
consistent, cost efficient solution to high volume event stream deduplication.

## API

The API consists of 3 operations available via JSON API or GRPC.
All operations take an array of strings (or bytes) and return an array of integers.
Each response integer represents a status code for the corresponding item in the batch.


**Lock**  [“aaa”, “bbb”, “ccc”, “ddd”, “eee”, “fff”]  
Response: [2, 2, 2, 2, 3, 4]  
Four were successfully *locked* (2) and will remain so for 30 seconds.  
One item was *busy* (3), and one already *exists* (4) in the database.


**Rollback**  [“aaa”]  
Response: [1]  
Here, one previously locked item is successfully rolled back to *open* (1).  
This is common (and necessary) when a client fails to process an item.


**Commit**  [“bbb”, “cccc”, “dddd”]  
Response: [4, 4, 4]  
These three locked items are committed to the database and show as *exists* (4).  
They will remain committed until they expire.

These three simple batch operations can be combined to implement an observable message deduplication system where duplicate delivery can be drastically reduced and exactly once delivery can be measured accurately.


## Terminology

The terminology used by this library is slightly unconventional. 
Rather than following conventional cluster/node/partition terminology, it borrows its deployment/host/node terminology from its current raft integration ([dragonboat](https://github.com/lni/dragonboat)).

![Architecture](https://i.imgur.com/eGC4ImB.png)

## How to run

Clone the source and run `make dev` to spin up a 3 node local cluster. Open `_examples/local_3x2/config.*.yml` to inspect and alter the expected directory configuration.

## Status

This repository is in pre-alpha. It has been made public for review. The master branch should work, but should not be deployed to staging environments until tagged v0.1.0

## Roadmap

### v0.1.0
- No migrations.
- No discovery.
- No recovery.
- No auth.
- Static pepper.
- Fixed expiration.
- HTTP/RPC interface.
- Lock propagation.
- Lock timeout.
- Rollback.
- Commit.
- Buckets.

## Requirements

### 1) Use a journaled filesystem

Journaling is [enabled by default](https://en.wikipedia.org/wiki/Comparison_of_file_systems#Features)
in xfs, ext3, ext4 and ntfs but it _is_ a setting that _might_ be disabled.

Use of a _writeback_ filesystem (the opposite of ordered/journaled) can cause database corruption on system crash
because writeback filesystems sometimes write pages to disk out of order while journaled filesystems do not.

You can use `debugfs` to ensure that your filesystem is configured with *has_journal* enabled.

```
> debugfs -R features /dev/sda1

debugfs 1.42.9 (28-Dec-2013)
Filesystem features: has_journal ext_attr resize_inode dir_index filetype needs_recovery extent 64bit
flex_bg sparse_super large_file huge_file uninit_bg dir_nlink extra_isize
```
