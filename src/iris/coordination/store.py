from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import asdict
from typing import Dict, Optional

from .models import MeetingThread


class ThreadStore(ABC):
    @abstractmethod
    def get(self, thread_id: str) -> Optional[MeetingThread]:
        raise NotImplementedError

    @abstractmethod
    def put(self, thread: MeetingThread) -> None:
        raise NotImplementedError


class InMemoryThreadStore(ThreadStore):
    def __init__(self) -> None:
        self._db: Dict[str, MeetingThread] = {}

    def get(self, thread_id: str) -> Optional[MeetingThread]:
        return self._db.get(thread_id)

    def put(self, thread: MeetingThread) -> None:
        self._db[thread.thread_id] = thread