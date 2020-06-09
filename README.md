# Shackle

Shackle is a horizontally scalable distributed hash set with two phase commit built on LMDB and Raft.

## Roadmap

### v0.1.0
- One shard, 1-N nodes.
- No partitioning.
- No migrations.
- No discovery.
- No pepper.
- No auth.
- Fixed expiration.
- Raft leader is shard leader.
- HTTP/RPC interface.
- Lock propagation.
- Lock timeout.
- Rollback.
- Commit.
