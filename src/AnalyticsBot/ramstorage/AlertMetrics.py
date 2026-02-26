from dataclasses import dataclass
from dataclasses import field

from typing import List
from typing import Tuple

@dataclass
class AlertMetrics:
    sum_rps_sl_1: float = 0.0
    sum_rps_sl_2: float = 0.0
    sum_rps_sl_3: float = 0.0
    sum_rps_sl_4: float = 0.0
    sum_rps_sl_5: float = 0.0
    sum_max_loss: float = 0.0
    sum_rpb_sl_1: float = 0.0
    sum_rpb_sl_2: float = 0.0
    sum_rpb_sl_3: float = 0.0
    sum_rpb_sl_4: float = 0.0
    sum_rpb_sl_5: float = 0.0
    sum_max_profit: float = 0.0

    top_loss: List[tuple[str, float]] = field(default_factory=list)
    top_profit: List[tuple[str, float]] = field(default_factory=list)

    tickers_count: int = 0

