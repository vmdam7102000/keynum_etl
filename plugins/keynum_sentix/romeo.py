"""The Romeo signal.

A pure translation of `build_strategy()` from the legacy
/home/keynum/strategy_sentix/romeo_sp500.py. No I/O here -- give it two pandas
Series and it returns a DataFrame. That is what makes it testable against the
legacy output (see tests/test_romeo.py and scripts/reconcile.py).

WHAT THE SIGNAL IS
------------------
A weekly four-state equity allocation derived from two Sentix S&P 500 survey
series. It is a state machine with hysteresis: the position persists until a
rule fires, and within one week the rules are evaluated in a fixed order with
later rules overriding earlier ones.

    SNTVSPI0  ->  "zdf"    (the legacy code calls it sp_zdf)
    SNTMSPI1  ->  "sent"   (the legacy code calls it sent)

    zdf_low   := zdf  < -0.3
    zdf_high  := zdf  >  0.2
    sent_low  := sent <= sent.rolling(4).min()     # a 4-week low
    sent_high := sent >= sent.rolling(26).max()    # a 26-week high

    for each week, in date order:
        if zdf_low:                  position =  1.0
        if not zdf_low and sent_low: position =  0.5
        if zdf_high:                 position =  0.0
        if sent_high:                position = -1.0

Meaning of the states -- read this before wiring it to an allocation:

     1.0  fully long
     0.5  half long
     0.0  FLAT (no position)
    -1.0  FULLY SHORT, not "out of the market"

Hans's brief described -1 as "out of market". In the legacy backtest -1
multiplies the return, i.e. it is full negative exposure, and 0 is the flat
state. This has to be confirmed before the signal drives real money -- see
README §4.4. If the intent turns out to be "flat", that is a rule change:
give it a new spec_version rather than editing history.

NO EXECUTION LAG IS APPLIED HERE
--------------------------------
`compute()` returns the signal as of each observation date. The legacy weekly
e-mail published exactly this -- `sent_strategy.position[-1]`, unshifted. The
`shift(1)` in the legacy script applied only to its backtest return column, and
the sibling script uses shift(2), shift(4) and shift(6) for three different
purposes. There is no single inherited "Romeo lag". Use `apply_execution_lag()`
and state the choice explicitly.
"""
from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd

ZDF_CODE = "SNTVSPI0"
SENT_CODE = "SNTMSPI1"


@dataclass(frozen=True)
class RomeoParams:
    """Every tunable in one place. Change one -> bump spec_version."""

    zdf_low_threshold: float = -0.3
    zdf_high_threshold: float = 0.2
    sent_low_window: int = 4
    sent_high_window: int = 26

    long_state: float = 1.0
    half_state: float = 0.5
    flat_state: float = 0.0
    # State entered when `sent_high` fires. -1 in Variant A (short); 0 in
    # Variant B, which therefore has no short state at all.
    sent_high_state: float = -1.0

    initial_state: float = 0.0
    # Applied with a strict '>' AFTER the rolling windows are computed, so the
    # retained rows have fully warmed-up windows. None = keep all history.
    start_after: str | None = "2014-01-01"

    spec_version: str = "romeo-A-1"
    source_codes: tuple[str, ...] = field(default=(ZDF_CODE, SENT_CODE))


#: The definition NordLB currently receives by e-mail. Use this one.
VARIANT_A = RomeoParams()

#: The AllMarkets variant. Different thresholds, no short state, longer history.
#: Provided for completeness -- do not publish it as "Romeo" without deciding
#: which definition is canonical (README §4.2).
VARIANT_B = RomeoParams(
    sent_high_window=35,
    half_state=0.4,
    sent_high_state=0.0,
    start_after="2002-01-01",
    spec_version="romeo-B-1",
)


class RomeoInputError(ValueError):
    pass


def _validate(zdf: pd.Series, sent: pd.Series, params: RomeoParams) -> None:
    for name, series in (("zdf", zdf), ("sent", sent)):
        if series.empty:
            raise RomeoInputError(f"{name} series is empty")
        if not isinstance(series.index, pd.DatetimeIndex):
            raise RomeoInputError(f"{name} must have a DatetimeIndex, got {type(series.index)}")
        if series.index.has_duplicates:
            dupes = series.index[series.index.duplicated()].unique()[:5].tolist()
            raise RomeoInputError(f"{name} has duplicate dates, e.g. {dupes}")

    # The legacy code built its frame on the zdf index and let pandas align the
    # rest. That is silently wrong if the two series ever diverge, so fail loud
    # instead: a missing week in one input would shift every later state.
    missing = zdf.index.symmetric_difference(sent.index)
    if len(missing):
        raise RomeoInputError(
            f"{ZDF_CODE} and {SENT_CODE} cover different dates "
            f"({len(missing)} mismatched, e.g. {missing[:5].tolist()}). "
            "Both series must be present for every survey week."
        )

    warmup = max(params.sent_low_window, params.sent_high_window)
    if len(zdf) < warmup:
        raise RomeoInputError(
            f"Need at least {warmup} observations to warm up the rolling windows, got {len(zdf)}"
        )


def compute(
    zdf: pd.Series,
    sent: pd.Series,
    params: RomeoParams = VARIANT_A,
) -> pd.DataFrame:
    """Compute the Romeo signal.

    Args:
        zdf:  SNTVSPI0 as a date-indexed Series.
        sent: SNTMSPI1 as a date-indexed Series.

    Returns:
        DataFrame indexed by observation date with columns:
            zdf, sent, zdf_low, zdf_high, sent_low, sent_high, position
        `position` is the unlagged signal. Rows before params.start_after are
        dropped, but they still contribute to the rolling windows.
    """
    zdf = zdf.sort_index().astype(float)
    sent = sent.sort_index().astype(float)
    _validate(zdf, sent, params)

    # Rolling windows over the FULL history, before any date cutoff -- this is
    # what the legacy code does, and it is why the first retained row already
    # has a valid 26-week maximum.
    frame = pd.DataFrame(index=zdf.index)
    frame["zdf"] = zdf
    frame["sent"] = sent
    frame["zdf_low"] = zdf < params.zdf_low_threshold
    frame["zdf_high"] = zdf > params.zdf_high_threshold
    frame["sent_low"] = sent <= sent.rolling(params.sent_low_window).min()
    frame["sent_high"] = sent >= sent.rolling(params.sent_high_window).max()

    if params.start_after is not None:
        frame = frame[frame.index > pd.Timestamp(params.start_after)]

    if frame.empty:
        raise RomeoInputError(f"No observations after {params.start_after}")

    positions: list[float] = []
    position = params.initial_state
    for row in frame.itertuples():
        # Order matters: later rules override earlier ones within the same week.
        if row.zdf_low:
            position = params.long_state
        if not row.zdf_low and row.sent_low:
            position = params.half_state
        if row.zdf_high:
            position = params.flat_state
        if row.sent_high:
            position = params.sent_high_state
        positions.append(position)

    frame["position"] = positions
    frame.index.name = "obs_date"
    return frame


def apply_execution_lag(positions: pd.Series, periods: int) -> pd.Series:
    """Shift the signal forward by `periods` observations.

    `compute()` publishes the current week's state. If the consumer cannot trade
    on the same bar the survey refers to, apply a lag here -- explicitly, once,
    at the point of use. Do not bake it into the stored signal.
    """
    if periods < 0:
        raise ValueError("Execution lag must be >= 0")
    return positions.shift(periods)


#: Presentation only. Kept apart from the computation on purpose -- the legacy
#: script entangled the two, which is why its sibling renders a 0.4 position as
#: three grey lights.
TRAFFIC_LIGHT = {
    1.0: "green",
    0.5: "amber-green",
    0.0: "amber",
    -1.0: "red",
}


def traffic_light(position: float) -> str:
    """Map a position to a traffic-light colour, or 'unknown' if unmapped."""
    return TRAFFIC_LIGHT.get(position, "unknown")


def latest(frame: pd.DataFrame) -> dict:
    """Summarise the most recent week, for a dashboard tile or an alert."""
    row = frame.iloc[-1]
    return {
        "obs_date": frame.index[-1].date(),
        "position": float(row["position"]),
        "traffic_light": traffic_light(float(row["position"])),
        "zdf": float(row["zdf"]),
        "sent": float(row["sent"]),
        "zdf_low": bool(row["zdf_low"]),
        "zdf_high": bool(row["zdf_high"]),
        "sent_low": bool(row["sent_low"]),
        "sent_high": bool(row["sent_high"]),
    }
