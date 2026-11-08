from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, StrictBool, StrictInt, model_validator

Stage = Literal['prepare', 'bake', 'pack']
STAGES = ('prepare', 'bake', 'pack')
DURATIONS = {'prepare': 2.0, 'bake': 8.0, 'pack': 2.0}
WORKERS = {'prep': 'prepare', 'baker-1': 'bake', 'baker-2': 'bake', 'pack': 'pack'}


class Task(BaseModel):
    model_config = ConfigDict(extra='forbid')
    order_id: str = Field(min_length=1)
    session_id: str = Field(min_length=1)
    label: str = Field(min_length=1)
    pizza_type: str = 'Margherita'
    failure_kind: Literal['none', 'invalid', 'transient'] = 'none'
    retry_count: int = Field(default=0, ge=0, le=2)
    force_redelivery: bool = False


class DisplayEvent(BaseModel):
    event_id: str = Field(min_length=1)
    session_id: str
    order_id: str
    stage: Stage
    status: Literal['delivered', 'redelivered', 'started', 'stage_completed', 'ready',
                    'handoff_confirmed', 'ack_sent', 'retry_scheduled', 'failed',
                    'effect_recorded', 'duplicate_skipped', 'crash_triggered', 'requeue_sent',
                    'processing_completed', 'reject_sent']
    worker_id: str
    attempt_id: str
    timestamp: float = Field(ge=0, allow_inf_nan=False)
    duration_s: float = Field(gt=0, allow_inf_nan=False)
    redelivered: bool
    delivery_attempt: int = Field(default=1, ge=1)
    retry_count: int = 0
    detail: str = ''


class ControlRequest(BaseModel):
    model_config = ConfigDict(extra='forbid')
    paused: StrictBool


class ControlResponse(BaseModel):
    session_id: str = Field(min_length=1)
    revision: StrictInt = Field(ge=0)
    paused: StrictBool
    duration_s: float = Field(default=2, ge=0.2, le=20)
    idempotency: bool = False
    retiring: bool = False
    crash_point: Literal['during', 'after_effect'] | None = None
    crash_token: str | None = None


class Heartbeat(BaseModel):
    model_config = ConfigDict(extra='forbid')
    instance_id: str = Field(min_length=1)
    sequence: StrictInt = Field(ge=1)
    observed_session_id: str | None = None
    observed_revision: StrictInt = Field(default=-1, ge=-1)
    active_order_id: str | None = None
    attempt_id: str | None = None
    broker_connected: StrictBool

    @model_validator(mode='after')
    def paired_activity(self):
        if bool(self.active_order_id) != bool(self.attempt_id):
            raise ValueError('active_order_id and attempt_id must both be present or absent')
        return self
