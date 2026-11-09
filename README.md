# Thread-Based Parallelism: Distributed Computing Example

This project demonstrates core **Thread-Based Parallelism** concepts applied to Parallel and Distributed Computing (PDC) scenarios. In a single file, `pdc_thread_concepts.py`, I implemented 3 concepts mirroring the structure of standard thread programming examples.

## Concepts

| Part | Concept | Scenario |
|------|---------|----------|
| 1 | Lock | Cloud Storage Quota Allocation (50 blocks, 80 requesting nodes) |
| 2 | Queue (Producer-Consumer) | Master/Worker MapReduce: A master queues data chunks, a worker processes them |
| 3 | Semaphore | Database Connection Pool (2 max connections, 6 microservices) |

### 1. Lock - Cloud Storage Quota Allocation
In distributed systems, when multiple nodes attempt to claim shared resources simultaneously, a **race condition** can occur[cite: 2]. Without a lock, the system over-provisions storage, leading to negative available blocks. Using `threading.Lock()`, only one node can update the quota table at a time, preventing over-allocation.

### 2. Queue - Master/Worker Task Processing
The `queue.Queue` object passes data safely between threads[cite: 2]. This simulates a MapReduce architecture where the Master Node (producer) assigns data chunks to the queue, and a Worker Node (consumer) retrieves and processes them safely in sequence. 

### 3. Semaphore - Database Connection Pool
Databases often limit concurrent connections to prevent crashing. Using `threading.Semaphore(2)` acts as a connection pool, allowing only 2 microservices to execute queries simultaneously[cite: 2]. The remaining services block and wait until a connection is returned to the pool. The program tracks and prints the peak concurrent connections (always 2 or fewer)[cite: 2].

SAMPLE OUTPUT:

WITHOUT lock -> allocated: 55, blocks left: -5
WITH lock    -> allocated: 50, blocks left: 0
...
Maximum concurrent database queries executed: 2

## How to Run
Requirement: Python 3.7+ (no extra libraries needed)[cite: 2]

```bash
python pdc_thread_concepts.py
