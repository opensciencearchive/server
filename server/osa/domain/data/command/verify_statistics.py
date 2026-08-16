"""Admin verifier for ``table_statistics`` (#219 phase 6).

The lockstep counts are load-bearing for every discovery surface; this command
is the trust-but-verify safety net that lets exact maintenance replace live
counting. It recomputes truth with the backfill migration's query, reports any
drift, and overwrites only when explicitly asked. Defence in depth — never
load-bearing, never scheduled.
"""

from osa.domain.auth.model.principal import Principal
from osa.domain.auth.model.role import Role
from osa.domain.data.model.statistics import StatisticsDrift
from osa.domain.data.port.statistics_store import StatisticsStore
from osa.domain.shared.authorization.gate import at_least
from osa.domain.shared.command import Command, CommandHandler, Result


class VerifyTableStatistics(Command):
    repair: bool = False


class StatisticsDriftReport(Result):
    drift: list[StatisticsDrift]
    repaired: bool


class VerifyTableStatisticsHandler(CommandHandler[VerifyTableStatistics, StatisticsDriftReport]):
    """Recompute → diff → report; mutate only on ``repair=True``."""

    __auth__ = at_least(Role.ADMIN)

    principal: Principal
    stats_store: StatisticsStore

    async def run(self, cmd: VerifyTableStatistics) -> StatisticsDriftReport:
        drift = await self.stats_store.table_statistics_drift()
        repaired = False
        if cmd.repair and drift:
            await self.stats_store.repair_table_statistics()
            repaired = True
        return StatisticsDriftReport(drift=drift, repaired=repaired)
