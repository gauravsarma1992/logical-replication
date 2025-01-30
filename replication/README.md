# Logical Replication in Golang

## Why yet another Raft project
For now, this is a hobby project where I have put up most of the components
required to implement logical replication on top of Raft. The repository is
built using Golang. I am calling it a hobby project for now since there are
edge cases which needs fixing, productionising the services and making it more
resilient. Most of the Raft repos out there are written to be mentioned in a
blog post with crappy modularity and no real life use cases and instead just
focuses on some functional blocks that they read from the Raft paper. 
I feel that understanding something and implementing something are different
things and I definitely understood the nuances while implementing this.
I am hoping I get time to productionise the entire service soon. Meanwhile,
I am going to be using this service as a replication service for my custom
file based blog.
It's going to be fun!

## Design
The overall service is broken down into submodules or `managers` which own
a particular piece of the functionality and try to handle things solely in
their domain. The idea is to span out these individual managers in the future
for more complex use cases when the need arises. Each manager should be
able to run on its own without the need for dependence or synchronicity on each other.

### Elements
- Replication Manager - This is the controller which interacts with all the other managers
- Transport Manager - Implements key functions for the transport library
- Node Manager - Has Node level verificiations and node methods
- Bootstrap Manager - Bootstraps the node by connecting with the right node and getting the required
cluster information
- Cluster Manager - Manages a representation of the cluster and the responsibilities of the other
nodes
- Health Manager - Tracks the health of the node periodically and raises issues when encountered. Deteriorating
health can be used to step down
- Heartbeat Manager - Heartbeats are sent and received across the full mesh and tracked to understand the
reachability of a node 
- Election Manager - Manages election responsibilities. All leader and follower transition decisions emanate
from this manager
- Data Replication Manager - Depending on the node's responsibility, it polls the WAL directly or a leader node
to fetch the latest log index

### Summary 
When a node goes down, replication allows other nodes to step in and takes its place leading to minimal downtime. This is done by creating a cluster or network of nodes with different purposes and control configurations. 
Leader replication points to a replication logic where there is a leader node and the leader is the only node in the cluster which can accept writes. The other nodes in the cluster can respond to read requests, thus leading to higher throughput in reads and also higher performance in writes as it reduces the read requests required to be responded by the leader node.

## Control Plane Phases
The Replication control plane consists of multiple phases.
Each phase provides particular requirements and functionalities to the node.

- Bootstrap Phase
- Data readiness Phase
- Run Phase
- Election Phase

### Bootup phase
While the node starts, it enters into a bootstrap phase where it identifies itself and its peer nodes.
Every node must be provided with a cluster hostname. The node first connects to the cluster hostname. The cluster hostname can be a reference to itself if it's a single node cluster, the hostname of any active node regardless of whether it's currently the master or not, or a DNS based load balancer. Even if a node connects to a follower node in the beginning, it will learn the correct master from all other nodes in the network.
- If there are no other nodes in the network, the node refers to itself as the leader and can start accepting writes.
- If there are other nodes in the network and there is a different leader, it will connect to the leader and start ramping up.

### Data readiness phase
In this phase, the data readiness phase describes the steps required for the node to be at the same level as other nodes and to start accepting writes or replicating writes.
In a single node cluster, the node needs to come up and ensure that everything in the WAL is up to date.
In a multi node cluster, the node needs to replicate the data from the leader. While replicating, if the difference between the data is already there in the leader’s WAL file, then only those need to be replicated. However, if the data is not currently present in the leader's WAL file as it has been checkpointed or exceeds the buffer, then the node needs to ask for a snapshot of the current data and start replicating only when the snapshotting is completed.
Once the WAL files are within a delta of a configured time threshold, the node comes out of the data phase.

### Data Replication Phase
The Replication phase defines how data is replicated from the leader to the followers and the constraints that it will be operating inside.
In some datastores, the leader waits for write receipts from the quorum majority and only then proceeds with committing the change. This ensures data loss is minimal as data is persisted in other nodes as well before the leader commits it. This also slows down the process of writing data as it has to wait for write receipts from other nodes as well.
The other form of quorum writes is for leaders to immediately commit to their store and let the followers read till the latest entry. This allows faster response times for leaders but reduces consistency constraints as well.
Data replication is done in the following process:
A write query comes in to the leader node and is stored in the WAL
The leader uses the last_read_index from the WAL file to read the newly entered data
The leader either broadcasts the information to all the nodes or the data is pulled by the follower node from the leader using the last_read_index of the follower node
Depending on the quorum write configuration, the leader should commit the last_read_index

### Heartbeats
As the data is ready in the node, the node starts to participate in the cluster with its identity.
In this phase, the heartbeats are started and broadcasted across the cluster periodically. Heartbeats are also received from other nodes.
Based on every discovered node, it is expected that the discovered nodes would also start sending their heartbeats to the source node. For every node, heartbeats should be monitored and unhealthy status or not receiving heartbeat should be treated as an event, especially if the node is the leader. Heartbeats are a method of sending concise self information frequently to other nodes so that other nodes can use this updated information to track and take decisions if required.

### Networking and RPC calls
The RPC layer which allows communication across different nodes for metadata management.
This is also called the Control plane. It listens on a different TCP port.
Different possible protocols
Plain TCP
Protobuf with HTTP2 (preferred)
Protobuf with TCP
Json based HTTP

It's a single threaded communication channel with multiplexed messages.
The structure of a Message can be found in the Interfaces section.
Each message has a MessageGroup attributed to it.
A MessageGroup denotes the separation of concerns with respect to other message groups.
Each Message group’s messages can be sent asynchronously without affecting the functionality of other message groups. However, inside a message group, messages have to be in sequence and synchronous.
This allows messages in other groups to not interfere with other messages and allows customized configuration per message group. For example, election messages should not block heartbeats and other cluster information. At the same time, election messages shouldn’t be received out of order. Election messages also may be accorded a higher priority which allows the networking layer to prioritise some message groups over others.

### Elections
#### Setting up a new cluster
When all the nodes come up in a fresh cluster, they try to connect to a remote node. Since it’s a new cluster, there are chances that the nodes may be deployed in a progressive manner. This means that there may be instances where there is only one node in the cluster for some time. To ensure that there is no waiting for the remaining nodes to be active since it’s a non-deterministic state, the single node can start accepting writes by creating an election and winning it since there are no other voters in the cluster. As other nodes come up in the system, they connect to the remote node in the bootstrap phase. While fetching the cluster information from the remote node, it discovers that there is already a leader node which allows the current node to move itself to a follower node.

#### (WIP) Multiple leaders in a cluster
There can be cases when there are multiple leaders in a cluster because there was a split brain scenario or a leader was network partitioned due to an outage during which another leader took its place and when the network partition was fixed, the leader tries to take its position again in the cluster. When the previous leader tries to join the cluster, it may tell the other nodes that it’s the leader, but the nodes cannot transition to a new leader since there is already an active leader with the majority of votes and a higher term ID.

#### Keeping in sync with the leader
Each leader maintains a timestamp. This timestamp is used to break contentions or to decide the correctness of a decision. The leader’s timestamp is replicated by all the followers. If there are other nodes which try to portray themselves as the leader with a lower timestamp, the follower discards their message and asks the duplicitous leader node to change itself to a follower. If a node receives a message with a higher timestamp, the node should be able to change its own version of the leader to the node with a higher timestamp.

#### Deciding when an election should take place
Elections take place when there is ambiguity in the leader. When a particular node isn’t able to successfully talk to a leader for some time, an election should be started. If the node receives information from another node regarding an unhealthy leader, it can attempt communication towards the leader and attempt an election if it wasn’t able to connect to a healthy leader.

#### Starting an election
In an election, a follower can change their status to a candidate and send a broadcast message to all the other nodes asking for votes. If the majority of the nodes reply in the affirmative, then the candidate changes their status to leader by publishing its latest log index, updating its timestamp.

#### Multiple candidates for leader selection
If there are no elections taking place and if the requesting node is healthy, then the nodes in the network can vote affirmative for the requesting node to become the leader. When there are multiple candidates for a leader in the network, the subsequent election request must be discarded and a new election with a new term should be started. To reduce conflicts between multiple candidates trying to become the leader, each candidate can try to become a leader only after a random time interval. This randomness reduces conflicts between candidates during an election.

#### Possible Election scenarios
Scenarios
- No leader node
    - One node comes up early and decides to become leader when other nodes are not active
    - One node decides to become leader with other nodes’ votes
- Healthy Leader present
    - One node goes rogue and tries to become leader. If log index and term index are higher than leader node, then leader transition happens
- Unhealthy Leader present
    - One node tracks it first and starts election and wins it before other nodes can also start an election
    - Multiple nodes track it and multiple elections are started. If there are multiple vote requests, then the node rejects all subsequent vote requests till the first vote election is timed out or rejected
    - If the candidate node receives a vote request from another node or if it receives a rejected vote from any node, it should reject the vote and cancel it’s candidature

### Integration with the WAL (WIP)
The current WAL logic asynchronously updates the WAL buffer file and the store.
- GetLogsAfter(logIndex LogIndex, logCount int) (logs []Log)
- Commit(logs []Log)
- GetSnapshot()



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