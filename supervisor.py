from contextlib import asynccontextmanager
import os
import re
import subprocess
import sys
from threading import RLock

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from contracts import Stage, WORKERS

lock = RLock()
processes = {}
roles = {}


class WorkerSpec(BaseModel):
    worker_id: str
    stage: Stage


def launch(worker_id, stage):
    if not re.fullmatch(r'[a-z][a-z0-9-]{0,39}', worker_id):
        raise HTTPException(422, 'Invalid worker ID')
    with lock:
        process = processes.get(worker_id)
        if process and process.poll() is None:
            raise HTTPException(409, 'Worker is already running')
        env = {**os.environ, 'STAGE': stage, 'WORKER_ID': worker_id}
        processes[worker_id] = subprocess.Popen([sys.executable, '-u', 'worker.py'], env=env)
        roles[worker_id] = stage


def stop_all():
    with lock:
        for process in processes.values():
            if process.poll() is None:
                process.kill()
        for process in processes.values():
            process.wait(timeout=5)


@asynccontextmanager
async def lifespan(app):
    # Registration/admission remains with the API. Startup always checks its flag.
    try:
        for worker_id, stage in WORKERS.items():
            launch(worker_id, stage)
        yield
    finally:
        stop_all()


app = FastAPI(lifespan=lifespan)


@app.get('/workers')
def workers():
    with lock:
        result = []
        for worker_id, process in processes.items():
            exit_code = process.poll()
            result.append({'worker_id': worker_id, 'stage': roles[worker_id],
                           'running': exit_code is None, 'pid': process.pid, 'exit_code': exit_code})
        return result


@app.post('/workers')
def start_worker(spec: WorkerSpec):
    launch(spec.worker_id, spec.stage)
    return {'started': spec.worker_id}


@app.post('/workers/{worker_id}/stop')
def stop_worker(worker_id: str):
    with lock:
        process = processes.get(worker_id)
        if not process:
            raise HTTPException(404, 'Unknown worker')
        if process.poll() is None:
            process.kill()  # Deliberate real process failure, including while busy.
            process.wait(timeout=5)
        return {'stopped': worker_id, 'exit_code': process.poll()}


@app.post('/stop-all')
def quiesce():
    stop_all()
    return {'stopped': True}


@app.post('/baseline')
def baseline():
    stop_all()
    with lock:
        processes.clear()
        roles.clear()
        for worker_id, stage in WORKERS.items():
            launch(worker_id, stage)
    return workers()
