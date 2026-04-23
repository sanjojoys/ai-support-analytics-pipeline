#!/usr/bin/env python3
"""Generate synthetic CSV data for an AI support analytics engineering project.

Outputs:
- raw/accounts.csv
- raw/users.csv
- raw/product_events.csv
- raw/support_conversations.csv
- raw/experiment_assignments.csv
- raw/account_daily_status.csv
- seeds/dim_date.csv
- seeds/event_mapping.csv

The generator is designed to create realistic relationships that are useful for
analytics engineering practice:
- users who connect their workspace within 24 hours have higher activation
- first AI suggestion acceptance within 7 days improves D28 retention
- escalation spikes in the last 14 days are associated with churn
- onboarding Variant B improves activation for SMB and mid-market accounts
- enterprise accounts create more support conversations and resolve faster when
  AI assist is used
"""

from __future__ import annotations

import argparse
import json
import math
import random
from collections import Counter, deque
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd


PLAN_CONFIG = {
    "smb": {
        "user_range": (4, 18),
        "support_mean": 30,
        "churn_prob": 0.12,
        "activation_bonus": 0.00,
        "retention_bonus": 0.00,
    },
    "mid_market": {
        "user_range": (12, 35),
        "support_mean": 65,
        "churn_prob": 0.09,
        "activation_bonus": 0.03,
        "retention_bonus": 0.03,
    },
    "enterprise": {
        "user_range": (30, 75),
        "support_mean": 130,
        "churn_prob": 0.05,
        "activation_bonus": 0.02,
        "retention_bonus": 0.05,
    },
}

INDUSTRIES = [
    "SaaS",
    "E-commerce",
    "Fintech",
    "Healthcare",
    "EdTech",
    "Travel",
    "Logistics",
    "Media",
    "Marketplace",
    "Professional Services",
]

REGIONS = ["NA", "EMEA", "APAC", "LATAM"]
COUNTRIES = {
    "NA": ["US", "CA"],
    "EMEA": ["DE", "FR", "NL", "UK", "ES", "IE"],
    "APAC": ["IN", "SG", "AU", "JP"],
    "LATAM": ["BR", "MX"],
}

CHANNELS = [
    "organic_search",
    "paid_search",
    "partner",
    "content",
    "referral",
    "direct",
]

ROLE_TYPES = ["admin", "agent", "manager", "analyst"]
ISSUE_TYPES = [
    "setup",
    "billing",
    "api",
    "bug",
    "workflow",
    "permissions",
    "reporting",
]
CHANNEL_TYPES = ["chat", "email", "web"]
BILLING_STATUS = ["active", "trial", "past_due", "churned"]
FEATURES = ["fin_assist", "reporting", "inbox", "workflows", "knowledge_base"]
PAGES = [
    "home",
    "settings",
    "inbox",
    "reports",
    "help_center",
    "ai_panel",
    "onboarding",
]

PREFIXES = [
    "Blue",
    "North",
    "Bright",
    "Peak",
    "Nova",
    "Cloud",
    "Swift",
    "Signal",
    "Prime",
    "Pulse",
    "Vector",
    "Lumen",
    "Echo",
    "Atlas",
    "Cedar",
    "Aurora",
]
SUFFIXES = [
    "Labs",
    "Works",
    "AI",
    "Systems",
    "Flow",
    "Support",
    "Stack",
    "Cloud",
    "Ops",
    "Tech",
    "HQ",
    "Desk",
]


@dataclass
class AccountProfile:
    account_id: str
    account_name: str
    created_at: datetime
    plan_tier: str
    industry: str
    region: str
    company_size_bucket: str
    billing_status: str
    churn_date: Optional[date]
    renewal_date: date


@dataclass
class UserProfile:
    user_id: str
    account_id: str
    signup_at: datetime
    role_type: str
    country: str
    acquisition_channel: str
    workspace_connected_at: Optional[datetime]
    invited_teammates_count: int
    experiment_name: str
    variant: str
    quick_connect_flag: int
    activated_flag: int
    first_conversation_at: Optional[datetime]
    first_ai_view_at: Optional[datetime]
    first_ai_accept_at: Optional[datetime]
    retained_d7_flag: int
    retained_d28_flag: int
    activity_end_at: datetime
    engagement_segment: str


def clamp(value: float, lower: float = 0.01, upper: float = 0.99) -> float:
    return max(lower, min(upper, value))


def choose(rng: random.Random, values: Sequence[str], weights: Optional[Sequence[float]] = None) -> str:
    return rng.choices(list(values), weights=weights, k=1)[0]


def rand_seconds(rng: random.Random, min_seconds: int, max_seconds: int) -> int:
    return rng.randint(min_seconds, max_seconds)


def random_datetime_between(rng: random.Random, start_dt: datetime, end_dt: datetime) -> datetime:
    if end_dt <= start_dt:
        return start_dt
    delta_seconds = int((end_dt - start_dt).total_seconds())
    return start_dt + timedelta(seconds=rng.randint(0, delta_seconds))


def random_time_on_day(rng: random.Random, day: date, hour_start: int = 7, hour_end: int = 21) -> datetime:
    hour = rng.randint(hour_start, hour_end)
    minute = rng.randint(0, 59)
    second = rng.randint(0, 59)
    return datetime.combine(day, time(hour=hour, minute=minute, second=second))


def iso_no_tz(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def build_account_name(rng: random.Random) -> str:
    return f"{choose(rng, PREFIXES)} {choose(rng, SUFFIXES)}"


def company_size_for_plan(rng: random.Random, plan_tier: str) -> str:
    if plan_tier == "smb":
        return choose(rng, ["1-10", "11-50", "51-200"], [0.25, 0.50, 0.25])
    if plan_tier == "mid_market":
        return choose(rng, ["51-200", "201-500", "501-1000"], [0.25, 0.50, 0.25])
    return choose(rng, ["501-1000", "1001-5000", "5000+"], [0.20, 0.45, 0.35])


def daterange(start_day: date, end_day: date) -> Iterable[date]:
    cursor = start_day
    while cursor <= end_day:
        yield cursor
        cursor += timedelta(days=1)


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


class IdFactory:
    def __init__(self, prefix: str) -> None:
        self.prefix = prefix
        self.value = 0

    def next(self) -> str:
        self.value += 1
        return f"{self.prefix}_{self.value:08d}"


class SyntheticGenerator:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.rng = random.Random(args.seed)
        self.np_rng = np.random.default_rng(args.seed)
        self.start_day = datetime.strptime(args.start_date, "%Y-%m-%d").date()
        self.end_day = self.start_day + timedelta(days=args.days - 1)
        self.end_dt = datetime.combine(self.end_day, time(23, 59, 59))

        self.account_id_factory = IdFactory("acct")
        self.user_id_factory = IdFactory("usr")
        self.event_id_factory = IdFactory("evt")
        self.session_id_factory = IdFactory("sess")
        self.conversation_id_factory = IdFactory("conv")

        self.accounts: List[AccountProfile] = []
        self.users: List[UserProfile] = []
        self.experiment_rows: List[dict] = []
        self.product_events: List[dict] = []
        self.support_conversations: List[dict] = []

        self.users_by_account: Dict[str, List[UserProfile]] = {}
        self.ai_accept_rate_by_account: Dict[str, float] = {}

    def run(self) -> None:
        self.generate_accounts()
        self.generate_users_and_assignments()
        self.generate_product_events()
        self.generate_support_conversations()
        self.inject_support_events_into_product_events()
        self.write_outputs()

    def generate_accounts(self) -> None:
        for _ in range(self.args.accounts):
            account_id = self.account_id_factory.next()
            plan_tier = choose(self.rng, ["smb", "mid_market", "enterprise"], [0.55, 0.30, 0.15])
            created_offset_days = self.rng.randint(0, max(5, min(45, self.args.days - 40)))
            created_dt = datetime.combine(self.start_day + timedelta(days=created_offset_days), time(9, 0, 0))
            churn_date: Optional[date] = None
            if self.rng.random() < PLAN_CONFIG[plan_tier]["churn_prob"]:
                churn_low = self.start_day + timedelta(days=max(40, int(self.args.days * 0.60)))
                churn_high = self.start_day + timedelta(days=max(50, self.args.days - 7))
                if churn_low > self.end_day:
                    churn_low = self.start_day + timedelta(days=max(5, self.args.days // 2))
                if churn_high > self.end_day:
                    churn_high = self.end_day
                if churn_low <= churn_high:
                    churn_date = churn_low + timedelta(days=self.rng.randint(0, (churn_high - churn_low).days))
            renewal_date = created_dt.date() + timedelta(days=90)
            billing_status = "trial" if (self.end_day - created_dt.date()).days < 14 else "active"
            if churn_date is not None and churn_date <= self.end_day:
                billing_status = "churned"
            elif self.rng.random() < 0.03:
                billing_status = "past_due"

            self.accounts.append(
                AccountProfile(
                    account_id=account_id,
                    account_name=build_account_name(self.rng),
                    created_at=created_dt,
                    plan_tier=plan_tier,
                    industry=choose(self.rng, INDUSTRIES),
                    region=choose(self.rng, REGIONS, [0.42, 0.32, 0.18, 0.08]),
                    company_size_bucket=company_size_for_plan(self.rng, plan_tier),
                    billing_status=billing_status,
                    churn_date=churn_date,
                    renewal_date=renewal_date,
                )
            )

    def generate_users_and_assignments(self) -> None:
        users_by_account: Dict[str, List[UserProfile]] = {}
        for account in self.accounts:
            low, high = PLAN_CONFIG[account.plan_tier]["user_range"]
            n_users = self.rng.randint(low, high)
            account_users: List[UserProfile] = []
            signup_window_end = min(self.end_day - timedelta(days=35), account.created_at.date() + timedelta(days=25))
            if signup_window_end <= account.created_at.date():
                signup_window_end = min(self.end_day - timedelta(days=1), account.created_at.date() + timedelta(days=5))
            for _ in range(n_users):
                user_id = self.user_id_factory.next()
                signup_day = account.created_at.date() + timedelta(
                    days=self.rng.randint(0, max(0, (signup_window_end - account.created_at.date()).days))
                )
                signup_at = random_time_on_day(self.rng, signup_day, 7, 19)
                country = choose(self.rng, COUNTRIES[account.region])
                acquisition = choose(
                    self.rng,
                    CHANNELS,
                    [0.22, 0.16, 0.10, 0.12, 0.18, 0.22],
                )
                role_type = choose(
                    self.rng,
                    ROLE_TYPES,
                    [0.20, 0.55, 0.15, 0.10] if account.plan_tier != "enterprise" else [0.15, 0.60, 0.15, 0.10],
                )
                variant = choose(self.rng, ["control", "variant_b"], [0.50, 0.50])
                experiment_name = "onboarding_ai_tooltip"

                p_connect = 0.72
                if acquisition in {"organic_search", "referral", "partner"}:
                    p_connect += 0.05
                if account.plan_tier == "enterprise":
                    p_connect -= 0.03
                if variant == "variant_b" and account.plan_tier in {"smb", "mid_market"}:
                    p_connect += 0.06
                connected = self.rng.random() < clamp(p_connect)

                workspace_connected_at: Optional[datetime] = None
                quick_connect_flag = 0
                if connected:
                    p_quick = 0.44
                    if acquisition in {"organic_search", "referral"}:
                        p_quick += 0.06
                    if role_type in {"admin", "manager"}:
                        p_quick += 0.05
                    if variant == "variant_b" and account.plan_tier in {"smb", "mid_market"}:
                        p_quick += 0.12
                    quick_connect_flag = int(self.rng.random() < clamp(p_quick))
                    if quick_connect_flag:
                        workspace_connected_at = signup_at + timedelta(seconds=rand_seconds(self.rng, 30 * 60, 24 * 3600))
                    else:
                        workspace_connected_at = signup_at + timedelta(seconds=rand_seconds(self.rng, 36 * 3600, 10 * 24 * 3600))
                    if workspace_connected_at > self.end_dt:
                        workspace_connected_at = None
                        quick_connect_flag = 0

                p_activate = 0.22 + PLAN_CONFIG[account.plan_tier]["activation_bonus"]
                if quick_connect_flag:
                    p_activate += 0.34
                elif workspace_connected_at is not None:
                    p_activate += 0.14
                if variant == "variant_b" and account.plan_tier in {"smb", "mid_market"}:
                    p_activate += 0.09
                if role_type in {"admin", "manager"}:
                    p_activate += 0.05
                activated_flag = int(self.rng.random() < clamp(p_activate, 0.02, 0.98))

                first_conversation_at: Optional[datetime] = None
                first_ai_view_at: Optional[datetime] = None
                first_ai_accept_at: Optional[datetime] = None
                if activated_flag:
                    base_dt = workspace_connected_at or signup_at
                    first_conversation_at = base_dt + timedelta(seconds=rand_seconds(self.rng, 2 * 3600, 5 * 24 * 3600))
                    p_ai_view = 0.72 if account.plan_tier != "enterprise" else 0.66
                    if self.rng.random() < p_ai_view:
                        first_ai_view_at = first_conversation_at + timedelta(seconds=rand_seconds(self.rng, 120, 3600))
                        p_ai_accept = 0.42 + (0.05 if quick_connect_flag else 0.0)
                        if account.plan_tier == "enterprise":
                            p_ai_accept += 0.06
                        if self.rng.random() < clamp(p_ai_accept):
                            first_ai_accept_at = first_ai_view_at + timedelta(seconds=rand_seconds(self.rng, 60, 2 * 24 * 3600))
                            if first_ai_accept_at > self.end_dt:
                                first_ai_accept_at = None

                first_ai_accept_within_7d = int(
                    first_ai_accept_at is not None and first_ai_accept_at <= signup_at + timedelta(days=7)
                )

                p_retained_d7 = 0.18 + PLAN_CONFIG[account.plan_tier]["retention_bonus"]
                if activated_flag:
                    p_retained_d7 += 0.24
                if quick_connect_flag:
                    p_retained_d7 += 0.08
                if variant == "variant_b" and account.plan_tier in {"smb", "mid_market"}:
                    p_retained_d7 += 0.04
                retained_d7_flag = int(self.rng.random() < clamp(p_retained_d7))

                p_retained_d28 = 0.10 + PLAN_CONFIG[account.plan_tier]["retention_bonus"]
                if activated_flag:
                    p_retained_d28 += 0.20
                if first_ai_accept_within_7d:
                    p_retained_d28 += 0.18
                if account.churn_date is not None:
                    p_retained_d28 -= 0.08
                retained_d28_flag = int(retained_d7_flag == 1 and self.rng.random() < clamp(p_retained_d28))

                if retained_d28_flag:
                    engagement_segment = choose(self.rng, ["medium", "high"], [0.45, 0.55])
                elif retained_d7_flag:
                    engagement_segment = choose(self.rng, ["low", "medium"], [0.45, 0.55])
                else:
                    engagement_segment = "low"

                if retained_d28_flag:
                    activity_span_days = self.rng.randint(35, max(40, self.args.days - 2))
                elif retained_d7_flag:
                    activity_span_days = self.rng.randint(10, 27)
                else:
                    activity_span_days = self.rng.randint(2, 10)

                activity_end_at = signup_at + timedelta(days=activity_span_days)
                if account.churn_date is not None:
                    churn_cutoff = datetime.combine(account.churn_date, time(12, 0, 0))
                    activity_end_at = min(activity_end_at, churn_cutoff)
                activity_end_at = min(activity_end_at, self.end_dt)

                invited_teammates_count = 0
                if role_type in {"admin", "manager"} and workspace_connected_at is not None:
                    invited_teammates_count = self.rng.randint(0, 6 if account.plan_tier == "smb" else 12)

                profile = UserProfile(
                    user_id=user_id,
                    account_id=account.account_id,
                    signup_at=signup_at,
                    role_type=role_type,
                    country=country,
                    acquisition_channel=acquisition,
                    workspace_connected_at=workspace_connected_at,
                    invited_teammates_count=invited_teammates_count,
                    experiment_name=experiment_name,
                    variant=variant,
                    quick_connect_flag=quick_connect_flag,
                    activated_flag=activated_flag,
                    first_conversation_at=first_conversation_at,
                    first_ai_view_at=first_ai_view_at,
                    first_ai_accept_at=first_ai_accept_at,
                    retained_d7_flag=retained_d7_flag,
                    retained_d28_flag=retained_d28_flag,
                    activity_end_at=activity_end_at,
                    engagement_segment=engagement_segment,
                )
                account_users.append(profile)
                self.users.append(profile)
                self.experiment_rows.append(
                    {
                        "experiment_id": f"exp_{account.account_id}",
                        "experiment_name": experiment_name,
                        "user_id": user_id,
                        "account_id": account.account_id,
                        "assigned_at": iso_no_tz(signup_at),
                        "variant": variant,
                    }
                )
            users_by_account[account.account_id] = account_users

        self.users_by_account = users_by_account
        self.ai_accept_rate_by_account = {
            account_id: (
                sum(1 for u in profiles if u.first_ai_accept_at is not None) / max(1, len(profiles))
            )
            for account_id, profiles in users_by_account.items()
        }

    def add_event(
        self,
        account_id: str,
        user_id: str,
        session_id: str,
        event_ts: datetime,
        event_name: str,
        feature_name: Optional[str] = None,
        page_name: Optional[str] = None,
        properties: Optional[dict] = None,
    ) -> None:
        self.product_events.append(
            {
                "event_id": self.event_id_factory.next(),
                "account_id": account_id,
                "user_id": user_id,
                "session_id": session_id,
                "event_ts": iso_no_tz(event_ts),
                "event_name": event_name,
                "feature_name": feature_name or "",
                "page_name": page_name or "",
                "properties_json": json.dumps(properties or {}, separators=(",", ":")),
                "ingested_at": iso_no_tz(event_ts + timedelta(minutes=self.rng.randint(1, 720))),
            }
        )

    def generate_product_events(self) -> None:
        for user in self.users:
            # Core milestones
            session_id = self.session_id_factory.next()
            self.add_event(
                user.account_id,
                user.user_id,
                session_id,
                user.signup_at,
                "signup_completed",
                page_name="onboarding",
                properties={"source": user.acquisition_channel},
            )

            if user.workspace_connected_at is not None:
                self.add_event(
                    user.account_id,
                    user.user_id,
                    session_id,
                    user.workspace_connected_at,
                    "workspace_connected",
                    feature_name="workflows",
                    page_name="settings",
                    properties={"quick_connect_flag": user.quick_connect_flag},
                )

            if user.invited_teammates_count > 0 and user.workspace_connected_at is not None:
                invite_ts = user.workspace_connected_at + timedelta(hours=self.rng.randint(1, 48))
                invite_ts = min(invite_ts, self.end_dt)
                self.add_event(
                    user.account_id,
                    user.user_id,
                    session_id,
                    invite_ts,
                    "teammate_invited",
                    feature_name="inbox",
                    page_name="settings",
                    properties={"invite_count": user.invited_teammates_count},
                )

            if user.first_conversation_at is not None:
                session_id = self.session_id_factory.next()
                self.add_event(
                    user.account_id,
                    user.user_id,
                    session_id,
                    user.first_conversation_at,
                    "conversation_created",
                    feature_name="inbox",
                    page_name="inbox",
                    properties={"milestone": True},
                )
            if user.first_ai_view_at is not None:
                self.add_event(
                    user.account_id,
                    user.user_id,
                    session_id,
                    user.first_ai_view_at,
                    "ai_suggestion_viewed",
                    feature_name="fin_assist",
                    page_name="ai_panel",
                    properties={"surface": "reply_assist"},
                )
            if user.first_ai_accept_at is not None:
                self.add_event(
                    user.account_id,
                    user.user_id,
                    session_id,
                    user.first_ai_accept_at,
                    "ai_suggestion_accepted",
                    feature_name="fin_assist",
                    page_name="ai_panel",
                    properties={"surface": "reply_assist"},
                )

            # Ongoing product usage
            active_days = max(1, (user.activity_end_at.date() - user.signup_at.date()).days)
            if user.engagement_segment == "high":
                # FIX: Ensure stop is at least 19
                n_days = min(active_days, self.rng.randint(18, max(19, min(60, active_days + 1))))
                sessions_per_day = (2, 4)
            elif user.engagement_segment == "medium":
                # FIX: Ensure stop is at least 9
                n_days = min(active_days, self.rng.randint(8, max(9, min(30, active_days + 1))))
                sessions_per_day = (1, 3)
            else:
                # FIX: Ensure stop is at least 3
                n_days = min(active_days, self.rng.randint(2, max(3, min(12, active_days + 1))))
                sessions_per_day = (1, 2)

                
            if active_days <= 0:
                continue

            sampled_offsets = set()
            while len(sampled_offsets) < n_days:
                # Beta distributions create either more front-loaded or more spread usage.
                if user.retained_d28_flag:
                    fraction = self.np_rng.beta(1.2, 1.2)
                elif user.retained_d7_flag:
                    fraction = self.np_rng.beta(1.1, 2.0)
                else:
                    fraction = self.np_rng.beta(0.9, 2.8)
                sampled_offsets.add(int(fraction * active_days))
                if len(sampled_offsets) > active_days:
                    break

            for offset in sorted(sampled_offsets):
                usage_day = min(user.signup_at.date() + timedelta(days=int(offset)), self.end_day)
                if usage_day < user.signup_at.date() or usage_day > user.activity_end_at.date():
                    continue
                day_sessions = self.rng.randint(sessions_per_day[0], sessions_per_day[1])
                for _ in range(day_sessions):
                    session_id = self.session_id_factory.next()
                    start_ts = random_time_on_day(self.rng, usage_day)
                    self.add_event(
                        user.account_id,
                        user.user_id,
                        session_id,
                        start_ts,
                        "dashboard_viewed",
                        feature_name="reporting",
                        page_name=choose(self.rng, ["home", "reports", "inbox"], [0.45, 0.35, 0.20]),
                        properties={"engagement_segment": user.engagement_segment},
                    )
                    if self.rng.random() < (0.72 if user.activated_flag else 0.18):
                        conv_ts = start_ts + timedelta(minutes=self.rng.randint(1, 25))
                        self.add_event(
                            user.account_id,
                            user.user_id,
                            session_id,
                            conv_ts,
                            "conversation_created",
                            feature_name="inbox",
                            page_name="inbox",
                            properties={"origin": "usage_session"},
                        )
                        if self.rng.random() < (0.64 if user.first_ai_view_at is not None else 0.40):
                            ai_view_ts = conv_ts + timedelta(minutes=self.rng.randint(1, 8))
                            self.add_event(
                                user.account_id,
                                user.user_id,
                                session_id,
                                ai_view_ts,
                                "ai_suggestion_viewed",
                                feature_name="fin_assist",
                                page_name="ai_panel",
                                properties={"surface": choose(self.rng, ["reply_assist", "summarize", "rewrite"], [0.6, 0.2, 0.2])},
                            )
                            p_accept = 0.32
                            if user.first_ai_accept_at is not None:
                                p_accept += 0.14
                            if user.engagement_segment == "high":
                                p_accept += 0.06
                            if self.rng.random() < clamp(p_accept):
                                self.add_event(
                                    user.account_id,
                                    user.user_id,
                                    session_id,
                                    ai_view_ts + timedelta(minutes=self.rng.randint(1, 5)),
                                    "ai_suggestion_accepted",
                                    feature_name="fin_assist",
                                    page_name="ai_panel",
                                    properties={"surface": "reply_assist"},
                                )
                    if self.rng.random() < 0.25:
                        export_ts = start_ts + timedelta(minutes=self.rng.randint(2, 30))
                        self.add_event(
                            user.account_id,
                            user.user_id,
                            session_id,
                            export_ts,
                            "report_exported",
                            feature_name="reporting",
                            page_name="reports",
                            properties={"format": choose(self.rng, ["csv", "pdf", "xlsx"], [0.45, 0.25, 0.30])},
                        )

    def generate_support_conversations(self) -> None:
        for account in self.accounts:
            account_users = self.users_by_account[account.account_id]
            ai_rate = self.ai_accept_rate_by_account.get(account.account_id, 0.0)
            base_mean = PLAN_CONFIG[account.plan_tier]["support_mean"]
            n_conversations = max(4, int(self.np_rng.poisson(base_mean)))
            if account.churn_date is not None:
                n_conversations += self.rng.randint(8, 20)

            churn_window_start = None
            if account.churn_date is not None:
                churn_window_start = account.churn_date - timedelta(days=14)

            for _ in range(n_conversations):
                in_churn_window = account.churn_date is not None and self.rng.random() < 0.30
                if in_churn_window and churn_window_start is not None:
                    created_day = churn_window_start + timedelta(days=self.rng.randint(0, 13))
                else:
                    created_day = self.start_day + timedelta(days=self.rng.randint(0, self.args.days - 1))
                if created_day > self.end_day:
                    created_day = self.end_day

                eligible = [u for u in account_users if u.signup_at.date() <= created_day]
                if not eligible:
                    continue
                user = choose(self.rng, eligible)
                created_at = random_time_on_day(self.rng, created_day)
                if created_at < user.signup_at:
                    created_at = user.signup_at + timedelta(minutes=self.rng.randint(5, 240))
                if account.churn_date is not None and created_at.date() > account.churn_date:
                    created_at = datetime.combine(account.churn_date, time(10, 0, 0))

                issue_type = choose(
                    self.rng,
                    ISSUE_TYPES,
                    [0.16, 0.12, 0.15, 0.18, 0.16, 0.11, 0.12],
                )
                channel = choose(self.rng, CHANNEL_TYPES, [0.55, 0.30, 0.15])
                ai_assist_prob = 0.30 + (0.35 * ai_rate)
                if account.plan_tier == "enterprise":
                    ai_assist_prob += 0.08
                ai_assist_used_flag = int(self.rng.random() < clamp(ai_assist_prob))

                escalated_prob = 0.10
                if issue_type in {"bug", "billing", "api"}:
                    escalated_prob += 0.08
                if ai_assist_used_flag:
                    escalated_prob -= 0.03
                if in_churn_window:
                    escalated_prob += 0.22
                escalated_flag = int(self.rng.random() < clamp(escalated_prob))

                base_resolution = {
                    "setup": 25,
                    "billing": 45,
                    "api": 70,
                    "bug": 95,
                    "workflow": 50,
                    "permissions": 35,
                    "reporting": 40,
                }[issue_type]
                if account.plan_tier == "enterprise":
                    base_resolution *= 0.90
                if ai_assist_used_flag:
                    base_resolution *= 0.72
                if escalated_flag:
                    base_resolution *= 1.8
                resolution_minutes = max(5, int(abs(self.np_rng.normal(base_resolution, max(8, base_resolution * 0.18)))))
                resolved_at = created_at + timedelta(minutes=resolution_minutes)
                if resolved_at > self.end_dt:
                    resolved_at = self.end_dt
                    resolution_minutes = int((resolved_at - created_at).total_seconds() / 60)

                if ai_assist_used_flag and not escalated_flag:
                    resolved_by = choose(self.rng, ["ai", "ai_plus_human"], [0.70, 0.30])
                elif ai_assist_used_flag:
                    resolved_by = "ai_plus_human"
                else:
                    resolved_by = "human"

                csat = 3.4
                if ai_assist_used_flag:
                    csat += 0.25
                if escalated_flag:
                    csat -= 0.75
                if resolution_minutes > 120:
                    csat -= 0.30
                csat += float(self.np_rng.normal(0.0, 0.35))
                csat = round(max(1.0, min(5.0, csat)), 1)

                self.support_conversations.append(
                    {
                        "conversation_id": self.conversation_id_factory.next(),
                        "account_id": account.account_id,
                        "user_id": user.user_id,
                        "created_at": iso_no_tz(created_at),
                        "resolved_at": iso_no_tz(resolved_at),
                        "channel": channel,
                        "issue_type": issue_type,
                        "resolved_by": resolved_by,
                        "ai_assist_used_flag": ai_assist_used_flag,
                        "escalated_flag": escalated_flag,
                        "resolution_minutes": resolution_minutes,
                        "csat_score": csat,
                    }
                )

    def inject_support_events_into_product_events(self) -> None:
        for row in self.support_conversations:
            account_id = row["account_id"]
            user_id = row["user_id"]
            session_id = self.session_id_factory.next()
            created_at = datetime.strptime(row["created_at"], "%Y-%m-%d %H:%M:%S")
            resolved_at = datetime.strptime(row["resolved_at"], "%Y-%m-%d %H:%M:%S")
            self.add_event(
                account_id,
                user_id,
                session_id,
                created_at,
                "ticket_created",
                feature_name="inbox",
                page_name="inbox",
                properties={
                    "channel": row["channel"],
                    "issue_type": row["issue_type"],
                    "escalated_flag": row["escalated_flag"],
                },
            )
            self.add_event(
                account_id,
                user_id,
                session_id,
                resolved_at,
                "conversation_resolved",
                feature_name="inbox",
                page_name="inbox",
                properties={
                    "resolved_by": row["resolved_by"],
                    "ai_assist_used_flag": row["ai_assist_used_flag"],
                },
            )

    def build_account_daily_status(self, product_events_df: pd.DataFrame, support_df: pd.DataFrame) -> pd.DataFrame:
        product_events_df = product_events_df.copy()
        support_df = support_df.copy()
        product_events_df["event_date"] = pd.to_datetime(product_events_df["event_ts"]).dt.date
        support_df["created_date"] = pd.to_datetime(support_df["created_at"]).dt.date

        event_user_pairs = (
            product_events_df[["account_id", "user_id", "event_date"]]
            .drop_duplicates()
            .groupby(["account_id", "event_date"])["user_id"]
            .apply(list)
            .to_dict()
        )

        ai_accepts_daily = (
            product_events_df.loc[product_events_df["event_name"] == "ai_suggestion_accepted"]
            .groupby(["account_id", "event_date"])["event_id"]
            .count()
            .to_dict()
        )

        conv_daily = support_df.groupby(["account_id", "created_date"])["conversation_id"].count().to_dict()
        esc_daily = support_df.groupby(["account_id", "created_date"])["escalated_flag"].sum().to_dict()

        rows: List[dict] = []
        for account in self.accounts:
            user_sets_window: deque[set] = deque()
            user_counter: Counter = Counter()
            conv_window: deque[int] = deque()
            esc_window: deque[int] = deque()
            ai_window: deque[int] = deque()
            conv_sum = 0
            esc_sum = 0
            ai_sum = 0

            for snapshot_day in daterange(self.start_day, self.end_day):
                users_today = set(event_user_pairs.get((account.account_id, snapshot_day), []))
                user_sets_window.append(users_today)
                for user_id in users_today:
                    user_counter[user_id] += 1

                conv_today = int(conv_daily.get((account.account_id, snapshot_day), 0))
                esc_today = int(esc_daily.get((account.account_id, snapshot_day), 0))
                ai_today = int(ai_accepts_daily.get((account.account_id, snapshot_day), 0))
                conv_window.append(conv_today)
                esc_window.append(esc_today)
                ai_window.append(ai_today)
                conv_sum += conv_today
                esc_sum += esc_today
                ai_sum += ai_today

                if len(user_sets_window) > 7:
                    users_out = user_sets_window.popleft()
                    for user_id in users_out:
                        user_counter[user_id] -= 1
                        if user_counter[user_id] <= 0:
                            del user_counter[user_id]
                    conv_sum -= conv_window.popleft()
                    esc_sum -= esc_window.popleft()
                    ai_sum -= ai_window.popleft()

                renewal_due_in_30d_flag = int(0 <= (account.renewal_date - snapshot_day).days <= 30)
                churned_flag = int(account.churn_date is not None and snapshot_day >= account.churn_date)
                rows.append(
                    {
                        "snapshot_date": snapshot_day.isoformat(),
                        "account_id": account.account_id,
                        "active_users_7d": len(user_counter),
                        "conversations_7d": conv_sum,
                        "ai_suggestions_accepted_7d": ai_sum,
                        "tickets_7d": conv_sum,
                        "escalations_7d": esc_sum,
                        "renewal_due_in_30d_flag": renewal_due_in_30d_flag,
                        "churned_flag": churned_flag,
                    }
                )

        return pd.DataFrame(rows)

    def build_dim_date_seed(self) -> pd.DataFrame:
        rows = []
        for day in daterange(self.start_day, self.end_day):
            rows.append(
                {
                    "date_day": day.isoformat(),
                    "year": day.year,
                    "month": day.month,
                    "day": day.day,
                    "day_of_week": day.weekday() + 1,
                    "week_start_date": (day - timedelta(days=day.weekday())).isoformat(),
                    "month_start_date": day.replace(day=1).isoformat(),
                    "is_weekend": int(day.weekday() >= 5),
                }
            )
        return pd.DataFrame(rows)

    def build_event_mapping_seed(self) -> pd.DataFrame:
        rows = [
            {"raw_event_name": "signup_completed", "canonical_event_name": "signup_completed", "event_group": "onboarding"},
            {"raw_event_name": "workspace_connected", "canonical_event_name": "workspace_connected", "event_group": "onboarding"},
            {"raw_event_name": "teammate_invited", "canonical_event_name": "teammate_invited", "event_group": "onboarding"},
            {"raw_event_name": "conversation_created", "canonical_event_name": "conversation_created", "event_group": "engagement"},
            {"raw_event_name": "ai_suggestion_viewed", "canonical_event_name": "ai_suggestion_viewed", "event_group": "ai"},
            {"raw_event_name": "ai_suggestion_accepted", "canonical_event_name": "ai_suggestion_accepted", "event_group": "ai"},
            {"raw_event_name": "ticket_created", "canonical_event_name": "ticket_created", "event_group": "support"},
            {"raw_event_name": "conversation_resolved", "canonical_event_name": "conversation_resolved", "event_group": "support"},
            {"raw_event_name": "dashboard_viewed", "canonical_event_name": "dashboard_viewed", "event_group": "analytics"},
            {"raw_event_name": "report_exported", "canonical_event_name": "report_exported", "event_group": "analytics"},
        ]
        return pd.DataFrame(rows)

    def write_outputs(self) -> None:
        outdir = Path(self.args.outdir)
        raw_dir = outdir / "raw"
        seeds_dir = outdir / "seeds"
        ensure_dir(raw_dir)
        ensure_dir(seeds_dir)

        accounts_df = pd.DataFrame(
            [
                {
                    "account_id": a.account_id,
                    "account_name": a.account_name,
                    "created_at": iso_no_tz(a.created_at),
                    "plan_tier": a.plan_tier,
                    "industry": a.industry,
                    "region": a.region,
                    "company_size_bucket": a.company_size_bucket,
                    "billing_status": a.billing_status,
                }
                for a in self.accounts
            ]
        )
        users_df = pd.DataFrame(
            [
                {
                    "user_id": u.user_id,
                    "account_id": u.account_id,
                    "signup_at": iso_no_tz(u.signup_at),
                    "role_type": u.role_type,
                    "country": u.country,
                    "acquisition_channel": u.acquisition_channel,
                    "workspace_connected_at": iso_no_tz(u.workspace_connected_at) if u.workspace_connected_at else "",
                    "invited_teammates_count": u.invited_teammates_count,
                }
                for u in self.users
            ]
        )
        product_events_df = pd.DataFrame(self.product_events).sort_values(["account_id", "user_id", "event_ts", "event_id"])
        support_df = pd.DataFrame(self.support_conversations).sort_values(["account_id", "created_at", "conversation_id"])
        experiment_df = pd.DataFrame(self.experiment_rows).sort_values(["account_id", "user_id"])
        account_daily_status_df = self.build_account_daily_status(product_events_df, support_df)
        dim_date_df = self.build_dim_date_seed()
        event_mapping_df = self.build_event_mapping_seed()

        accounts_df.to_csv(raw_dir / "accounts.csv", index=False)
        users_df.to_csv(raw_dir / "users.csv", index=False)
        product_events_df.to_csv(raw_dir / "product_events.csv", index=False)
        support_df.to_csv(raw_dir / "support_conversations.csv", index=False)
        experiment_df.to_csv(raw_dir / "experiment_assignments.csv", index=False)
        account_daily_status_df.to_csv(raw_dir / "account_daily_status.csv", index=False)
        dim_date_df.to_csv(seeds_dir / "dim_date.csv", index=False)
        event_mapping_df.to_csv(seeds_dir / "event_mapping.csv", index=False)

        summary = {
            "accounts": int(len(accounts_df)),
            "users": int(len(users_df)),
            "product_events": int(len(product_events_df)),
            "support_conversations": int(len(support_df)),
            "experiment_assignments": int(len(experiment_df)),
            "account_daily_status": int(len(account_daily_status_df)),
            "dim_date": int(len(dim_date_df)),
            "event_mapping": int(len(event_mapping_df)),
            "output_directory": str(outdir.resolve()),
            "seed": self.args.seed,
        }
        (outdir / "generation_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
        print(json.dumps(summary, indent=2))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate synthetic CSV data for an AI support analytics project.")
    parser.add_argument("--outdir", default="./data", help="Output directory where raw/ and seeds/ will be created.")
    parser.add_argument("--accounts", type=int, default=500, help="Number of accounts to generate.")
    parser.add_argument("--days", type=int, default=120, help="Number of days to cover in the generated data.")
    parser.add_argument("--start-date", default="2025-01-01", help="Start date in YYYY-MM-DD format.")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducible generation.")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    SyntheticGenerator(args).run()


if __name__ == "__main__":
    main()
