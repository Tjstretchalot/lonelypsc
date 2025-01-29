from dataclasses import dataclass
from typing import TYPE_CHECKING, Type

from lonelypsc.client import PubSubNotifyResult


@dataclass
class HttpPubSubNotifyResult:
    notified: int


if TYPE_CHECKING:
    _: Type[PubSubNotifyResult] = HttpPubSubNotifyResult
