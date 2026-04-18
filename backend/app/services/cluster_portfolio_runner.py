"""Orchestration helpers for Cluster Portfolio cycle runs and notifications."""

from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy.ext.asyncio import AsyncSession

from app.services.cluster_portfolio import (
    ClusterPortfolioEngine,
    PortfolioCycleResult,
    PortfolioValuation,
)
from app.services.cluster_portfolio_notifier import (
    ClusterPortfolioNotifier,
    NotificationAttempt,
)


@dataclass(frozen=True)
class ClusterPortfolioRunOutcome:
    """Full outcome for one run, including optional notification attempts."""

    cycle_result: PortfolioCycleResult
    valuation: PortfolioValuation | None
    notifications: list[NotificationAttempt]


async def run_cluster_portfolio_cycle(
    db: AsyncSession,
    *,
    dry_run: bool = False,
    send_emails: bool = False,
    email_test_replay_latest: bool = False,
) -> ClusterPortfolioRunOutcome:
    """Run one Cluster Portfolio cycle and optionally notify on resulting actions."""
    engine = ClusterPortfolioEngine()
    notifier = ClusterPortfolioNotifier()

    cycle_result = await engine.run_cycle(db, dry_run=dry_run)

    notifications: list[NotificationAttempt] = []
    if not dry_run and send_emails:
        notifications = await notifier.notify_cycle(
            db,
            portfolio_id=cycle_result.portfolio_id,
            portfolio_name=cycle_result.portfolio_name,
            buys_opened=cycle_result.buys_opened,
            sells_closed=cycle_result.sells_closed,
            replay_latest=email_test_replay_latest,
        )

    if dry_run:
        return ClusterPortfolioRunOutcome(
            cycle_result=cycle_result,
            valuation=None,
            notifications=notifications,
        )

    await db.commit()

    portfolio = await engine.get_default_portfolio(db)
    if portfolio is None:
        raise RuntimeError("Cluster Portfolio missing after successful run")

    valuation = await engine.get_summary(db, portfolio)
    return ClusterPortfolioRunOutcome(
        cycle_result=cycle_result,
        valuation=valuation,
        notifications=notifications,
    )
