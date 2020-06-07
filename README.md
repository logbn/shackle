# Shackle

A horizontally scalable hash registry database with expiration and two phase commit built on LMDB and Raft.

## Roadmap

0.1 One shard, 1-N nodes.
	No partitioning.
	No migrations.
	No discovery.
	No pepper.
	No auth.
	Fixed expiration.
	Raft leader is shard leader.
	HTTP/RPC interface.
	Lock propagation.
	Lock timeout.
	Rollback.
	Commit.
