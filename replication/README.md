# Replication

## Testing
Testing in a distributed setup is more challenging since we also have to spend time
to build the right kind of infrastructure to support end to end testing on a single
machine.
We need to a way to seamlessly create and tear down cluster mimicing setups to truly
understand if the system is working appropriately.

### Requirements
- Being able to spawn multiple nodes on the same machine with different network identification
- Have different configuration options for the nodes
- Induce different health based events specifically for a node 
- Simulate elections
- Simulate WAL read/write and rollback operations

## TODO
- Election interfaces
- Health tracking
- WAL interfaces