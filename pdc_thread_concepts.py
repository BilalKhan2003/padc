"""
Thread-Based Parallelism - Distributed Computing Examples
Structure based on original thread_concepts.py
"""
import queue
import threading
import time

# ---------------------------------------------------------------
# PART 1: LOCK - Cloud Storage Quota Allocation
# ---------------------------------------------------------------
TOTAL_STORAGE_BLOCKS = 50
CONCURRENT_NODES = 80  # More nodes requesting blocks than available


def allocate_storage(use_lock):
    """80 distributed nodes try to claim 50 storage blocks concurrently."""
    state = {"blocks_available": TOTAL_STORAGE_BLOCKS, "allocated": 0}
    lock = threading.Lock()

    def node_request():
        def try_allocate():
            if state["blocks_available"] > 0:
                time.sleep(0.001)  # simulates network/processing delay
                state["blocks_available"] -= 1
                state["allocated"] += 1

        if use_lock:
            with lock:
                try_allocate()
        else:
            try_allocate()

    threads = [threading.Thread(target=node_request) for _ in range(CONCURRENT_NODES)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return state


def run_storage_demo():
    print("=== Concept 1: Lock (Cloud Storage Allocation) ===")
    print(f"Total Blocks: {TOTAL_STORAGE_BLOCKS}, Requesting Nodes: {CONCURRENT_NODES}")
    s = allocate_storage(use_lock=False)
    print(f"WITHOUT lock -> allocated: {s['allocated']}, blocks left: {s['blocks_available']}")
    s = allocate_storage(use_lock=True)
    print(f"WITH lock    -> allocated: {s['allocated']}, blocks left: {s['blocks_available']}\n")


if __name__ == "__main__":
    run_storage_demo()
    
    
# ---------------------------------------------------------------
# PART 2: QUEUE - Master/Worker Node Task Processing (Producer-Consumer)
# ---------------------------------------------------------------
DATA_CHUNKS = ["Dataset_Part_1", "Dataset_Part_2", "Dataset_Part_3", 
               "Dataset_Part_4", "Dataset_Part_5", "Dataset_Part_6"]


def master_node(task_queue):
    """Producer: Master node assigns data chunks to the queue."""
    for i, chunk in enumerate(DATA_CHUNKS, start=1):
        task_queue.put((i, chunk))
        print(f"[Master] Queued MapReduce task #{i}: {chunk}")
        time.sleep(0.1)
    task_queue.put(None)  # signal for the worker to stop


def worker_node(task_queue):
    """Consumer: Worker node fetches and processes data chunks."""
    while True:
        task = task_queue.get()
        if task is None:
            print("[Worker] Task queue empty. Node going to sleep.")
            break
        task_id, chunk = task
        print(f"[Worker] Processing task #{task_id} ({chunk})...")
        time.sleep(0.25)
        print(f"[Worker] Completed task #{task_id}!")


def run_mapreduce_demo():
    print("=== Concept 2: Queue (Master/Worker Task Queue) ===")
    task_queue = queue.Queue()
    t1 = threading.Thread(target=master_node, args=(task_queue,))
    t2 = threading.Thread(target=worker_node, args=(task_queue,))
    t1.start()
    t2.start()
    t1.join()
    t2.join()
    print()


if __name__ == "__main__":
    # Based on the progressive execution pattern from the original script
    run_storage_demo()
    run_mapreduce_demo()


# ---------------------------------------------------------------
# PART 3: SEMAPHORE - Database Connection Pool (Limited Connections)
# ---------------------------------------------------------------
MICROSERVICES = ["AuthService", "BillingService", "UserService", "LogService", "EmailService", "AnalyticsService"]
MAX_DB_CONNECTIONS = 2  # The database can only handle 2 concurrent queries


def microservice(name, connection_pool, counter, counter_lock):
    """A microservice waits for a DB connection, executes a query, then releases it."""
    print(f"[{name}] is requesting a database connection")
    with connection_pool:  # semaphore: only MAX_DB_CONNECTIONS active at once
        with counter_lock:
            counter["now"] += 1
            counter["max"] = max(counter["max"], counter["now"])
        print(f"[{name}] acquired connection (Active queries: {counter['now']})")
        time.sleep(0.5)  # simulating database query execution
        with counter_lock:
            counter["now"] -= 1
        print(f"[{name}] query complete, connection released")


def run_db_pool_demo():
    print("=== Concept 3: Semaphore (Database Connection Pool) ===")
    print(f"Max DB Connections: {MAX_DB_CONNECTIONS}, Microservices: {len(MICROSERVICES)}")
    connection_pool = threading.Semaphore(MAX_DB_CONNECTIONS)
    counter = {"now": 0, "max": 0}
    counter_lock = threading.Lock()
    
    threads = [
        threading.Thread(target=microservice, args=(n, connection_pool, counter, counter_lock))
        for n in MICROSERVICES
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    print(f"Maximum concurrent database queries executed: {counter['max']}")


if __name__ == "__main__":
    run_storage_demo()
    run_mapreduce_demo()
    run_db_pool_demo()