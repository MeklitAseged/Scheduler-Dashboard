
import threading
import time
import random
import collections
from dataclasses import dataclass, field
from typing import List, Optional
import math


@dataclass
class Process:
    pid: int
    name: str
    arrival_time: float
    burst_time: float          # total CPU needed
    priority: int = 1          # lower = higher priority
    remaining_time: float = 0.0
    start_time: Optional[float] = None
    finish_time: Optional[float] = None
    waiting_time: float = 0.0
    turnaround_time: float = 0.0
    response_time: Optional[float] = None
    state: str = "ready"       # ready | running | waiting | done

    def __post_init__(self):
        self.remaining_time = self.burst_time


@dataclass
class SchedulerMetrics:
    throughput: float = 0.0          # processes / second
    avg_waiting_time: float = 0.0    # mean waiting time
    avg_turnaround_time: float = 0.0 # mean turnaround time
    avg_response_time: float = 0.0   # mean response time
    cpu_utilization: float = 0.0     # % CPU busy
    completed: int = 0
    timeline: list = field(default_factory=list)  # Gantt data


#  Bounded Buffer 

class BoundedBuffer:
    """
    Thread-safe bounded buffer using semaphores (classic producer-consumer).
    Producers (process generators) deposit into the buffer;
    the CPU (consumer) picks them up for execution.
    """

    def __init__(self, capacity: int = 8):
        self.capacity = capacity
        self.buffer: collections.deque = collections.deque()
        self._mutex = threading.Semaphore(1)          # mutual exclusion
        self._empty_slots = threading.Semaphore(capacity)  # available slots
        self._full_slots = threading.Semaphore(0)         # filled slots
        self._lock = threading.Lock()                 # for safe len() reads

    def produce(self, process: Process, timeout: float = 2.0) -> bool:
        """Add a process to the buffer. Returns False on timeout."""
        acquired = self._empty_slots.acquire(timeout=timeout)
        if not acquired:
            return False
        self._mutex.acquire()
        self.buffer.append(process)
        self._mutex.release()
        self._full_slots.release()
        return True

    def consume(self, timeout: float = 1.0) -> Optional[Process]:
        """Remove and return a process. Returns None on timeout."""
        acquired = self._full_slots.acquire(timeout=timeout)
        if not acquired:
            return None
        self._mutex.acquire()
        process = self.buffer.popleft()
        self._mutex.release()
        self._empty_slots.release()
        return process

    def peek_all(self) -> List[Process]:
        with self._lock:
            return list(self.buffer)

    def size(self) -> int:
        with self._lock:
            return len(self.buffer)


#  Scheduler Algorithms 

class Scheduler:
    """
    Simulates FCFS, SJF, Priority and Round Robin schedulers.
    Uses a BoundedBuffer as the ready queue and semaphores for
    CPU mutual exclusion (only one process runs at a time).
    """

    ALGORITHMS = ["FCFS", "SJF", "Priority", "Round Robin"]
    QUANTUM = 2.0  # time slice for RR (seconds in simulation)
    SPEED = 5.0    # simulation speed multiplier

    def __init__(self):
        self.buffer = BoundedBuffer(capacity=10)
        self._cpu_sem = threading.Semaphore(1)   # only 1 process on CPU
        self._result_lock = threading.Lock()
        self.metrics = SchedulerMetrics()
        self.completed_processes: List[Process] = []
        self.running_process: Optional[Process] = None
        self.algorithm: str = "FCFS"
        self.simulation_time: float = 0.0
        self.busy_time: float = 0.0
        self.is_running: bool = False
        self._pid_counter = 0
        self._threads: List[threading.Thread] = []
        self._stop_event = threading.Event()
        self.history: List[dict] = []   # timestamped snapshots for charts

    # Process Factory 

    def _next_pid(self) -> int:
        self._pid_counter += 1
        return self._pid_counter

    def generate_process(self) -> Process:
        pid = self._next_pid()
        burst = round(random.uniform(1.0, 6.0), 2)
        priority = random.randint(1, 5)
        p = Process(
            pid=pid,
            name=f"P{pid}",
            arrival_time=self.simulation_time,
            burst_time=burst,
            priority=priority,
        )
        return p

    #  Producer Thread 

    def _producer(self):
        """Periodically generates processes and deposits them in the buffer."""
        while not self._stop_event.is_set():
            wait = random.uniform(0.4, 1.2) / self.SPEED
            time.sleep(wait)
            if self._stop_event.is_set():
                break
            p = self.generate_process()
            p.state = "ready"
            success = self.buffer.produce(p, timeout=0.5)
            if success:
                self._record_event("arrive", p)

    #  CPU Consumer Thread

    def _get_next_process(self, queue: List[Process]) -> Optional[Process]:
        if not queue:
            return None
        if self.algorithm == "FCFS":
            return queue.pop(0)
        elif self.algorithm == "SJF":
            queue.sort(key=lambda x: x.remaining_time)
            return queue.pop(0)
        elif self.algorithm == "Priority":
            queue.sort(key=lambda x: x.priority)
            return queue.pop(0)
        elif self.algorithm == "Round Robin":
            return queue.pop(0)
        return queue.pop(0)

    def _cpu_worker(self):
        """Consumes processes from buffer and executes them on the CPU."""
        rr_queue: List[Process] = []

        while not self._stop_event.is_set():
            # Drain buffer into local ready queue
            while True:
                p = self.buffer.consume(timeout=0.05)
                if p is None:
                    break
                rr_queue.append(p)

            if not rr_queue:
                time.sleep(0.05)
                continue

            proc = self._get_next_process(rr_queue)
            if proc is None:
                continue

            # Acquire CPU semaphore (mutual exclusion)
            self._cpu_sem.acquire()
            try:
                self.running_process = proc
                proc.state = "running"

                if proc.start_time is None:
                    proc.start_time = self.simulation_time
                    proc.response_time = proc.start_time - proc.arrival_time

                exec_slice = (
                    min(self.QUANTUM, proc.remaining_time)
                    if self.algorithm == "Round Robin"
                    else proc.remaining_time
                )
                real_sleep = exec_slice / self.SPEED
                t0 = time.time()

                self._record_event("start", proc)

                time.sleep(real_sleep)
                elapsed = (time.time() - t0) * self.SPEED

                proc.remaining_time = max(0.0, proc.remaining_time - elapsed)
                self.busy_time += elapsed
                self.simulation_time += elapsed

                if proc.remaining_time <= 0.01:
                    proc.state = "done"
                    proc.finish_time = self.simulation_time
                    proc.turnaround_time = proc.finish_time - proc.arrival_time
                    proc.waiting_time = proc.turnaround_time - proc.burst_time
                    with self._result_lock:
                        self.completed_processes.append(proc)
                    self._update_metrics()
                    self._record_event("finish", proc)
                else:
                    proc.state = "ready"
                    rr_queue.append(proc)  # re-queue for RR
                    self._record_event("preempt", proc)

                self.running_process = None
            finally:
                self._cpu_sem.release()

    # Metrics 

    def _update_metrics(self):
        with self._result_lock:
            done = self.completed_processes
            n = len(done)
            if n == 0:
                return
            m = self.metrics
            m.completed = n
            m.avg_waiting_time = sum(p.waiting_time for p in done) / n
            m.avg_turnaround_time = sum(p.turnaround_time for p in done) / n
            m.avg_response_time = sum(
                p.response_time for p in done if p.response_time is not None
            ) / n
            elapsed = max(self.simulation_time, 0.001)
            m.throughput = n / elapsed
            m.cpu_utilization = min(100.0, (self.busy_time / elapsed) * 100)

    def _record_event(self, event_type: str, proc: Process):
        snap = {
            "t": round(self.simulation_time, 2),
            "event": event_type,
            "pid": proc.pid,
            "name": proc.name,
            "burst": proc.burst_time,
            "remaining": round(proc.remaining_time, 2),
            "waiting": round(proc.waiting_time, 2),
            "state": proc.state,
            "metrics": {
                "throughput": round(self.metrics.throughput, 3),
                "avg_wait": round(self.metrics.avg_waiting_time, 2),
                "avg_turnaround": round(self.metrics.avg_turnaround_time, 2),
                "avg_response": round(self.metrics.avg_response_time, 2),
                "cpu_util": round(self.metrics.cpu_utilization, 1),
                "completed": self.metrics.completed,
            },
        }
        self.history.append(snap)

    #  Lifecycle 

    def start(self, algorithm: str = "FCFS"):
        if self.is_running:
            self.stop()
        self.algorithm = algorithm
        self._stop_event.clear()
        self.metrics = SchedulerMetrics()
        self.completed_processes = []
        self.history = []
        self.simulation_time = 0.0
        self.busy_time = 0.0
        self._pid_counter = 0
        self.buffer = BoundedBuffer(capacity=10)
        self.is_running = True

        producer_t = threading.Thread(target=self._producer, daemon=True)
        cpu_t = threading.Thread(target=self._cpu_worker, daemon=True)
        self._threads = [producer_t, cpu_t]
        producer_t.start()
        cpu_t.start()

    def stop(self):
        self._stop_event.set()
        self.is_running = False
        for t in self._threads:
            t.join(timeout=1.0)
        self._threads = []

    def get_state(self) -> dict:
        self._update_metrics()
        m = self.metrics
        running = None
        if self.running_process:
            p = self.running_process
            running = {
                "pid": p.pid, "name": p.name,
                "burst": p.burst_time,
                "remaining": round(p.remaining_time, 2),
            }
        ready_queue = [
            {"pid": p.pid, "name": p.name, "burst": p.burst_time,
             "remaining": round(p.remaining_time, 2), "priority": p.priority}
            for p in self.buffer.peek_all()
        ]
        done_list = [
            {"pid": p.pid, "name": p.name,
             "burst": round(p.burst_time, 2),
             "wait": round(p.waiting_time, 2),
             "turnaround": round(p.turnaround_time, 2),
             "response": round(p.response_time or 0, 2)}
            for p in list(self.completed_processes)[-20:]
        ]
        return {
            "algorithm": self.algorithm,
            "is_running": self.is_running,
            "sim_time": round(self.simulation_time, 2),
            "buffer_size": self.buffer.size(),
            "running": running,
            "ready_queue": ready_queue,
            "completed": done_list,
            "metrics": {
                "throughput": round(m.throughput, 3),
                "avg_waiting_time": round(m.avg_waiting_time, 2),
                "avg_turnaround_time": round(m.avg_turnaround_time, 2),
                "avg_response_time": round(m.avg_response_time, 2),
                "cpu_utilization": round(m.cpu_utilization, 1),
                "completed_count": m.completed,
            },
            "history": self.history[-200:],
        }


# Global singleton
scheduler = Scheduler()