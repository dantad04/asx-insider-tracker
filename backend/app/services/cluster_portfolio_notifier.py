"""Email notifier for Cluster Portfolio buy/sell events."""

from __future__ import annotations

import asyncio
import logging
import smtplib
from dataclasses import dataclass
from email.message import EmailMessage
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.cluster_portfolio import ClusterPortfolioEvent
from app.services.cluster_portfolio import EVENT_BUY, EVENT_SELL, PortfolioAction

logger = logging.getLogger(__name__)

EVENT_EMAIL_FAILED = "email_failed"


@dataclass(frozen=True)
class NotificationAttempt:
    """Result of one email notification attempt."""

    trade_event_type: str
    ticker: str
    position_id: int | None
    status: str
    replayed: bool
    error: str | None = None


class ClusterPortfolioNotifier:
    """Send buy/sell notifications without affecting portfolio execution."""

    async def notify_cycle(
        self,
        db: AsyncSession,
        *,
        portfolio_id: int,
        portfolio_name: str,
        buys_opened: list[PortfolioAction],
        sells_closed: list[PortfolioAction],
        replay_latest: bool = False,
    ) -> list[NotificationAttempt]:
        work_items = await self._build_work_items(
            db,
            portfolio_id=portfolio_id,
            buys_opened=buys_opened,
            sells_closed=sells_closed,
            replay_latest=replay_latest,
        )
        if not work_items:
            return []

        recipients = self._recipients()
        if not recipients:
            logger.warning("Cluster Portfolio email enabled but CLUSTER_PORTFOLIO_EMAIL_TO is empty")
            return [
                await self._record_failure(
                    db,
                    portfolio_id=portfolio_id,
                    position_id=item["position_id"],
                    portfolio_name=portfolio_name,
                    item=item,
                    error="missing_recipient",
                )
                for item in work_items
            ]

        if not settings.smtp_host or not settings.smtp_from:
            logger.warning("Cluster Portfolio email enabled but SMTP env is incomplete")
            return [
                await self._record_failure(
                    db,
                    portfolio_id=portfolio_id,
                    position_id=item["position_id"],
                    portfolio_name=portfolio_name,
                    item=item,
                    error="missing_smtp_configuration",
                )
                for item in work_items
            ]

        attempts: list[NotificationAttempt] = []
        for item in work_items:
            payload = self._email_payload(
                portfolio_name=portfolio_name,
                item=item,
                recipients=recipients,
            )
            subject = self._subject(item["trade_event_type"], payload)
            body = self._body(item["trade_event_type"], payload)

            try:
                await asyncio.to_thread(
                    self._send_email_sync,
                    recipients,
                    subject,
                    body,
                )
                await self._record_email_event(
                    db,
                    portfolio_id=portfolio_id,
                    position_id=item["position_id"],
                    event_type="email_sent",
                    payload=payload,
                )
                attempts.append(
                    NotificationAttempt(
                        trade_event_type=item["trade_event_type"],
                        ticker=payload.get("ticker", "?"),
                        position_id=item["position_id"],
                        status="sent",
                        replayed=item["replayed"],
                    )
                )
            except Exception as exc:
                logger.warning(
                    "Cluster Portfolio email failed for %s %s: %s",
                    item["trade_event_type"],
                    payload.get("ticker", "?"),
                    exc,
                )
                attempts.append(
                    await self._record_failure(
                        db,
                        portfolio_id=portfolio_id,
                        position_id=item["position_id"],
                        portfolio_name=portfolio_name,
                        item=item,
                        error=str(exc),
                        recipients=recipients,
                    )
                )
        return attempts

    async def _build_work_items(
        self,
        db: AsyncSession,
        *,
        portfolio_id: int,
        buys_opened: list[PortfolioAction],
        sells_closed: list[PortfolioAction],
        replay_latest: bool,
    ) -> list[dict[str, Any]]:
        work_items: list[dict[str, Any]] = []
        for action in [*buys_opened, *sells_closed]:
            if action.position_id is None:
                continue
            payload = await self._load_trade_event_payload(
                db,
                portfolio_id=portfolio_id,
                position_id=action.position_id,
                trade_event_type=action.event_type,
            )
            if payload:
                work_items.append(
                    {
                        "trade_event_type": action.event_type,
                        "position_id": action.position_id,
                        "payload": payload,
                        "replayed": False,
                    }
                )

        if work_items or not replay_latest:
            return work_items

        replay_item = await self._load_latest_trade_event(db, portfolio_id=portfolio_id)
        return [replay_item] if replay_item else []

    async def _load_trade_event_payload(
        self,
        db: AsyncSession,
        *,
        portfolio_id: int,
        position_id: int,
        trade_event_type: str,
    ) -> dict[str, Any] | None:
        result = await db.execute(
            select(ClusterPortfolioEvent.payload_json)
            .where(
                ClusterPortfolioEvent.portfolio_id == portfolio_id,
                ClusterPortfolioEvent.position_id == position_id,
                ClusterPortfolioEvent.event_type == trade_event_type,
            )
            .order_by(ClusterPortfolioEvent.id.desc())
            .limit(1)
        )
        payload = result.scalar_one_or_none()
        return payload or None

    async def _load_latest_trade_event(
        self,
        db: AsyncSession,
        *,
        portfolio_id: int,
    ) -> dict[str, Any] | None:
        result = await db.execute(
            select(ClusterPortfolioEvent)
            .where(
                ClusterPortfolioEvent.portfolio_id == portfolio_id,
                ClusterPortfolioEvent.event_type.in_([EVENT_BUY, EVENT_SELL]),
            )
            .order_by(ClusterPortfolioEvent.id.desc())
            .limit(1)
        )
        event = result.scalar_one_or_none()
        if event is None or event.payload_json is None:
            return None
        return {
            "trade_event_type": event.event_type,
            "position_id": event.position_id,
            "payload": event.payload_json,
            "replayed": True,
        }

    async def _record_failure(
        self,
        db: AsyncSession,
        *,
        portfolio_id: int,
        position_id: int | None,
        portfolio_name: str,
        item: dict[str, Any],
        error: str,
        recipients: list[str] | None = None,
    ) -> NotificationAttempt:
        payload = self._email_payload(
            portfolio_name=portfolio_name,
            item=item,
            recipients=recipients or self._recipients(),
        )
        payload["error"] = error
        await self._record_email_event(
            db,
            portfolio_id=portfolio_id,
            position_id=position_id,
            event_type=EVENT_EMAIL_FAILED,
            payload=payload,
        )
        return NotificationAttempt(
            trade_event_type=item["trade_event_type"],
            ticker=payload.get("ticker", "?"),
            position_id=position_id,
            status="failed",
            replayed=item["replayed"],
            error=error,
        )

    async def _record_email_event(
        self,
        db: AsyncSession,
        *,
        portfolio_id: int,
        position_id: int | None,
        event_type: str,
        payload: dict[str, Any],
    ) -> None:
        db.add(
            ClusterPortfolioEvent(
                portfolio_id=portfolio_id,
                position_id=position_id,
                event_type=event_type,
                payload_json=payload,
            )
        )

    def _recipients(self) -> list[str]:
        raw = settings.cluster_portfolio_email_to or ""
        return [part.strip() for part in raw.split(",") if part.strip()]

    def _email_payload(
        self,
        *,
        portfolio_name: str,
        item: dict[str, Any],
        recipients: list[str],
    ) -> dict[str, Any]:
        payload = dict(item["payload"])
        payload.update(
            {
                "portfolio_name": portfolio_name,
                "notification_trade_event_type": item["trade_event_type"],
                "notification_replayed": item["replayed"],
                "email_to": recipients,
            }
        )
        return payload

    def _subject(self, trade_event_type: str, payload: dict[str, Any]) -> str:
        prefix = "Buy Opened" if trade_event_type == EVENT_BUY else "Sell Closed"
        suffix = " [Replay]" if payload.get("notification_replayed") else ""
        return f"Cluster Portfolio {prefix}: {payload.get('ticker', '?')}{suffix}"

    def _body(self, trade_event_type: str, payload: dict[str, Any]) -> str:
        action_label = "Buy opened" if trade_event_type == EVENT_BUY else "Sell closed"
        return "\n".join(
            [
                f"{action_label} for {payload.get('ticker', '?')}",
                "",
                f"Portfolio: {payload.get('portfolio_name')}",
                f"Ticker: {payload.get('ticker')}",
                f"Entry cluster ID: {payload.get('entry_cluster_id') or payload.get('cluster_id')}",
                f"Industry: {payload.get('industry')}",
                f"Director count: {payload.get('director_count')}",
                f"Total value (AUD): {payload.get('total_value_aud')}",
                f"Pct market cap: {payload.get('pct_market_cap')}",
                f"Buy date: {payload.get('buy_date')}",
                f"Sell date: {payload.get('sell_date')}",
                f"Entry price: {payload.get('entry_price')}",
                f"Entry price date: {payload.get('entry_price_date')}",
                f"Exit price: {payload.get('exit_price')}",
                f"Exit price date: {payload.get('exit_price_date')}",
                f"Planned exit date: {payload.get('planned_exit_date')}",
                f"Reason: {payload.get('reason')}",
                f"Buy reason: {payload.get('buy_reason')}",
                f"Replay: {payload.get('notification_replayed')}",
            ]
        )

    def _send_email_sync(
        self,
        recipients: list[str],
        subject: str,
        body: str,
    ) -> None:
        message = EmailMessage()
        message["Subject"] = subject
        message["From"] = settings.smtp_from
        message["To"] = ", ".join(recipients)
        message.set_content(body)

        if settings.smtp_use_ssl:
            with smtplib.SMTP_SSL(
                settings.smtp_host,
                settings.smtp_port,
                timeout=settings.smtp_timeout_seconds,
            ) as server:
                self._smtp_login(server)
                server.send_message(message)
            return

        with smtplib.SMTP(
            settings.smtp_host,
            settings.smtp_port,
            timeout=settings.smtp_timeout_seconds,
        ) as server:
            if settings.smtp_use_tls:
                server.starttls()
            self._smtp_login(server)
            server.send_message(message)

    def _smtp_login(self, server: smtplib.SMTP) -> None:
        if settings.smtp_username:
            server.login(settings.smtp_username, settings.smtp_password or "")
