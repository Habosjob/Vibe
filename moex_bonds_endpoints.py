from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class EndpointSpec:
    """Описание endpoint ISS для probe по облигациям."""

    name: str
    path_template: str
    params: dict[str, Any] = field(default_factory=dict)


BASE_ISS_PARAMS: dict[str, Any] = {"iss.meta": "off", "limit": 100}


def curated_bond_endpoint_specs() -> list[EndpointSpec]:
    """Возвращает curated-набор endpoint'ов ISS по облигациям (без комбинаторной генерации)."""

    return [
        EndpointSpec(
            name="securities",
            path_template="/iss/securities/{secid}.json",
            params={
                "iss.only": "securities,description,boards",
                "iss.meta": "on",
            },
        ),
        EndpointSpec(name="marketdata", path_template="/iss/engines/stock/markets/bonds/securities/{secid}.json"),
        EndpointSpec(name="history", path_template="/iss/history/engines/stock/markets/bonds/securities/{secid}.json"),
        EndpointSpec(name="candles", path_template="/iss/engines/stock/markets/bonds/securities/{secid}/candles.json"),
        EndpointSpec(name="trades", path_template="/iss/engines/stock/markets/bonds/securities/{secid}/trades.json"),
        EndpointSpec(name="bondization", path_template="/iss/securities/{secid}/bondization.json"),
        EndpointSpec(name="history_yields", path_template="/iss/history/engines/stock/markets/bonds/yields/{secid}.json"),
        EndpointSpec(name="boards", path_template="/iss/engines/stock/markets/bonds/boards.json"),
        EndpointSpec(name="security_aggregates", path_template="/iss/securities/{secid}/aggregates.json"),
        EndpointSpec(name="security_indices", path_template="/iss/securities/{secid}/indices.json"),
        EndpointSpec(name="securities_list", path_template="/iss/engines/stock/markets/bonds/securities.json"),
        EndpointSpec(name="dates", path_template="/iss/history/engines/stock/markets/bonds/securities/{secid}/dates.json"),
        EndpointSpec(name="orderbook", path_template="/iss/engines/stock/markets/bonds/securities/{secid}/orderbook.json"),
    ]
