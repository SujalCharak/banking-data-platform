from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

from bankdp.reference import (
    CURRENCIES,
    FIRST_NAMES,
    LAST_NAMES,
    MCC_PROFILE,
    MERCHANT_SUFFIX,
    MERCHANT_WORDS,
)

DAY = 86_400


@dataclass(frozen=True)
class GenConfig:
    customers: int = 4000
    start: date = date(2025, 1, 1)
    days: int = 365
    seed: int = 7
    out_dir: Path = Path("data")
    schema_drift_day: int = 120
    format_drift_day: int = 180


def make_iban(bank: str, number: int, country: str = "NL") -> str:
    bban = f"{bank}{number:010d}"
    digits = "".join(str(int(ch, 36)) for ch in bban + country + "00")
    return f"{country}{98 - int(digits) % 97:02d}{bban}"


class BankDataGenerator:
    def __init__(self, cfg: GenConfig):
        self.cfg = cfg
        self.rng = np.random.default_rng(cfg.seed)
        self.start = np.datetime64(cfg.start, "D")
        self.issues: list[pd.DataFrame] = []
        self.anomalies: list[pd.DataFrame] = []

    def run(self) -> dict:
        customers, customer_state = self._customers()
        accounts, acc = self._accounts(customer_state)
        merchants = self._merchants()
        fx = self._fx_rates()
        txns = self._transactions(acc, merchants)

        landing = self.cfg.out_dir / "landing"
        self._write_daily(customers, landing / "customers")
        self._write_daily(accounts, landing / "accounts")
        self._write_daily(merchants.assign(file_day=0), landing / "merchants")
        self._write_daily(fx, landing / "fx_rates")
        self._write_transactions(txns, landing / "transactions")

        truth = self.cfg.out_dir / "ground_truth"
        truth.mkdir(parents=True, exist_ok=True)
        pd.concat(self.anomalies).to_csv(truth / "injected_anomalies.csv", index=False)
        pd.concat(self.issues).to_csv(truth / "injected_issues.csv", index=False)

        summary = {
            "customers": int(customer_state.shape[0]),
            "customer_rows": int(customers.shape[0]),
            "accounts": int(acc.shape[0]),
            "merchants": int(merchants.shape[0]),
            "transaction_rows": int(txns.shape[0]),
            "issues": pd.concat(self.issues)["issue"].value_counts().to_dict(),
            "anomalies": pd.concat(self.anomalies)["rule"].value_counts().to_dict(),
        }
        (truth / "summary.json").write_text(json.dumps(summary, indent=2))
        return summary

    def _ts(self, seconds: np.ndarray) -> pd.Series:
        dt = self.start + seconds.astype("timedelta64[s]")
        return pd.Series(np.datetime_as_string(dt, unit="s")).str.replace("T", " ", regex=False)

    def _date(self, days: np.ndarray) -> pd.Series:
        return pd.Series(np.datetime_as_string(self.start + days.astype("timedelta64[D]"), unit="D"))

    def _customers(self) -> tuple[pd.DataFrame, pd.DataFrame]:
        rng, n, days = self.rng, self.cfg.customers, self.cfg.days
        first = rng.choice(FIRST_NAMES, n)
        last = rng.choice(LAST_NAMES, n)
        existing = rng.random(n) < 0.9
        created_day = np.where(existing, -rng.integers(30, 3650, n), rng.integers(0, days - 1, n))
        base = pd.DataFrame(
            {
                "customer_id": [f"C{i:07d}" for i in range(1, n + 1)],
                "first_name": first,
                "last_name": last,
                "email": [
                    f"{fn.lower()}.{ln.lower().replace(' ', '')}{i}@example.com"
                    for i, (fn, ln) in enumerate(zip(first, last, strict=True), start=1)
                ],
                "birth_date": self._date(-rng.integers(18 * 365, 80 * 365, n)),
                "country_code": rng.choice(["NL", "BE", "DE", "FR", "GB"], n, p=[0.72, 0.1, 0.08, 0.06, 0.04]),
                "segment": rng.choice(["RETAIL", "PRIVATE", "BUSINESS"], n, p=[0.85, 0.05, 0.10]),
                "risk_rating": rng.choice(["LOW", "MEDIUM", "HIGH"], n, p=[0.8, 0.17, 0.03]),
                "kyc_status": rng.choice(["VERIFIED", "PENDING"], n, p=[0.97, 0.03]),
                "created_day": created_day,
            }
        )
        created_s = created_day * DAY + rng.integers(8 * 3600, 18 * 3600, n)
        base["created_at"] = self._ts(created_s)
        base["updated_at"] = base["created_at"]
        base["file_day"] = np.where(created_day < 0, 0, created_day + 1)

        options = {
            "risk_rating": ["LOW", "MEDIUM", "HIGH"],
            "segment": ["RETAIL", "PRIVATE", "BUSINESS"],
            "kyc_status": ["VERIFIED", "PENDING", "EXPIRED"],
            "country_code": ["NL", "BE", "DE", "FR", "GB"],
        }
        rows = []
        for idx in np.flatnonzero(rng.random(n) < 0.2):
            state = base.iloc[idx].to_dict()
            lo = max(state["created_day"], 0) + 1
            if lo >= days - 1:
                continue
            for change_day in np.sort(rng.integers(lo, days - 1, rng.integers(1, 3))):
                field = rng.choice(list(options) + ["email"])
                if field == "email":
                    state["email"] = state["email"].replace("@example.com", "@mail.example.com")
                else:
                    state[field] = rng.choice([v for v in options[field] if v != state[field]])
                state["updated_at"] = self._ts(np.array([change_day * DAY + rng.integers(0, DAY)]))[0]
                state["file_day"] = change_day + 1
                rows.append(dict(state))
        cdc = pd.concat([base, pd.DataFrame(rows)], ignore_index=True)
        cdc = cdc.sort_values(["customer_id", "updated_at"]).reset_index(drop=True)

        messy = rng.random(len(cdc))
        cdc.loc[messy < 0.02, "email"] = "  " + cdc.loc[messy < 0.02, "email"].str.upper()
        cdc.loc[(messy >= 0.02) & (messy < 0.03), "country_code"] = cdc["country_code"].str.lower()
        dupes = cdc[rng.random(len(cdc)) < 0.005]
        cdc = pd.concat([cdc, dupes], ignore_index=True)
        self.issues.append(pd.DataFrame({"entity": "customers", "key": dupes["customer_id"], "issue": "DUPLICATE_ROW"}))

        return cdc.drop(columns=["created_day"]), base[["customer_id", "segment", "created_day"]]

    def _accounts(self, cust: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        rng, days = self.rng, self.cfg.days
        seg = cust["segment"].to_numpy()
        n = len(cust)
        p_savings = np.select([seg == "PRIVATE", seg == "BUSINESS"], [0.9, 0.3], 0.55)
        p_credit = np.select([seg == "PRIVATE", seg == "BUSINESS"], [0.8, 0.5], 0.3)
        parts = [
            pd.DataFrame({"cust_idx": np.arange(n), "account_type": "CHECKING", "currency": "EUR", "primary": True}),
            pd.DataFrame(
                {"cust_idx": np.flatnonzero(rng.random(n) < p_savings), "account_type": "SAVINGS", "currency": "EUR"}
            ),
            pd.DataFrame(
                {"cust_idx": np.flatnonzero(rng.random(n) < p_credit), "account_type": "CREDIT_CARD", "currency": "EUR"}
            ),
        ]
        fx_owner = np.flatnonzero((seg == "BUSINESS") & (rng.random(n) < 0.25))
        parts.append(
            pd.DataFrame(
                {
                    "cust_idx": fx_owner,
                    "account_type": "CHECKING",
                    "currency": rng.choice(list(CURRENCIES), len(fx_owner)),
                }
            )
        )
        acc = pd.concat(parts, ignore_index=True).sort_values("cust_idx", kind="stable").reset_index(drop=True)
        acc["primary"] = acc["primary"].eq(True)
        m = len(acc)
        acc["account_id"] = [f"A{i:07d}" for i in range(1, m + 1)]
        acc["customer_id"] = cust["customer_id"].to_numpy()[acc["cust_idx"]]
        acc["segment"] = seg[acc["cust_idx"]]
        acc["iban"] = [make_iban("DEMO", i) for i in range(1, m + 1)]

        created = cust["created_day"].to_numpy()[acc["cust_idx"]]
        opened = created + np.where(acc["primary"], 0, rng.integers(0, 120, m))
        opened = np.minimum(opened, days - 2)
        acc["opened_day"] = opened

        opening = np.select(
            [acc["account_type"] == "SAVINGS", acc["account_type"] == "CREDIT_CARD"],
            [rng.lognormal(np.log(9000), 1.0, m), -rng.uniform(0, 1500, m)],
            rng.lognormal(np.log(2500), 0.9, m),
        )
        acc["opening_balance"] = np.where(opened < 0, np.round(opening, 2), 0.0)

        end = np.full(m, days)
        roll = rng.random(m)
        status_change = np.where(roll < 0.02, "CLOSED", np.where(roll < 0.025, "FROZEN", ""))
        lo = np.maximum(opened, 0) + 30
        can_change = (status_change != "") & (lo < days - 1) & ~acc["primary"].to_numpy()
        change_day = np.where(can_change, rng.integers(np.minimum(lo, days - 2), days - 1), -1)
        end = np.where(can_change, change_day, end)
        acc["end_day"] = end

        open_s = opened * DAY + rng.integers(9 * 3600, 17 * 3600, m)
        base = pd.DataFrame(
            {
                "account_id": acc["account_id"],
                "customer_id": acc["customer_id"],
                "iban": acc["iban"],
                "account_type": acc["account_type"],
                "currency": acc["currency"],
                "status": "ACTIVE",
                "opened_date": self._date(opened),
                "closed_date": "",
                "opening_balance": acc["opening_balance"].map("{:.2f}".format),
                "updated_at": self._ts(open_s),
                "file_day": np.where(opened < 0, 0, opened + 1),
            }
        )
        changed = base[can_change].copy()
        cd = change_day[can_change]
        changed["status"] = status_change[can_change]
        changed["closed_date"] = np.where(changed["status"] == "CLOSED", self._date(cd), "")
        changed["updated_at"] = self._ts(cd * DAY + rng.integers(0, DAY, len(cd))).to_numpy()
        changed["file_day"] = cd + 1
        rows = pd.concat([base, changed], ignore_index=True).sort_values(["account_id", "updated_at"])
        return rows.reset_index(drop=True), acc

    def _merchants(self) -> pd.DataFrame:
        rng, n = self.rng, 1500
        mccs = list(MCC_PROFILE)
        weights = np.array([MCC_PROFILE[m][1] for m in mccs], dtype=float)
        mcc = rng.choice(mccs, n, p=weights / weights.sum())
        names = [f"{a} {b}" for a, b in zip(rng.choice(MERCHANT_WORDS, n), rng.choice(MERCHANT_SUFFIX, n), strict=True)]
        df = pd.DataFrame(
            {
                "merchant_id": [f"M{i:06d}" for i in range(1, n + 1)],
                "merchant_name": names,
                "mcc": mcc,
                "country_code": rng.choice(
                    ["NL", "BE", "DE", "FR", "GB", "US"], n, p=[0.8, 0.06, 0.05, 0.04, 0.03, 0.02]
                ),
            }
        )
        df.loc[rng.random(n) < 0.01, "merchant_name"] = df["merchant_name"] + "   "
        df["popularity"] = np.array([MCC_PROFILE[m][1] for m in mcc]) * (rng.pareto(1.5, n) + 1)
        df["amount_mult"] = np.array([MCC_PROFILE[m][0] for m in mcc])
        return df

    def _fx_rates(self) -> pd.DataFrame:
        rng, days = self.rng, self.cfg.days
        day_idx = np.arange(-10, days + 7)
        frames = []
        for ccy, base_rate in CURRENCIES.items():
            walk = base_rate * np.exp(np.cumsum(rng.normal(0, 0.004, len(day_idx))))
            frames.append(pd.DataFrame({"day": day_idx, "currency": ccy, "rate_to_eur": np.round(walk, 6)}))
        fx = pd.concat(frames, ignore_index=True)
        weekday = (self.start + fx["day"].to_numpy().astype("timedelta64[D]")).astype("datetime64[D]").view("int64")
        fx = fx[((weekday + 3) % 7) < 5].copy()
        fx["rate_date"] = self._date(fx["day"].to_numpy()).to_numpy()
        fx["source"] = "ECB_DEMO"
        fx["file_day"] = np.maximum(fx["day"] + 1, 0)
        return fx[["rate_date", "currency", "rate_to_eur", "source", "file_day"]]

    def _poisson_events(self, mask: np.ndarray, lam: np.ndarray, acc: pd.DataFrame, weekday_factor=None) -> tuple:
        days = self.cfg.days
        d = np.arange(days)
        idx = np.flatnonzero(mask)
        opened = np.maximum(acc["opened_day"].to_numpy()[idx], 0)
        end = acc["end_day"].to_numpy()[idx]
        active = (d[None, :] >= opened[:, None]) & (d[None, :] < end[:, None])
        rate = lam[:, None] * (weekday_factor[None, :] if weekday_factor is not None else 1.0)
        counts = self.rng.poisson(rate) * active
        flat = np.repeat(np.arange(counts.size), counts.ravel())
        return idx[flat // days], flat % days

    def _monthly_days(self, day_of_month: int | None = None, last: bool = False) -> np.ndarray:
        dates = pd.date_range(self.cfg.start, periods=self.cfg.days, freq="D")
        pick = dates.is_month_end if last else dates.day == day_of_month
        return np.flatnonzero(pick)

    def _scheduled(self, acc_idx: np.ndarray, days_: np.ndarray, acc: pd.DataFrame) -> tuple:
        a = np.repeat(acc_idx, len(days_))
        d = np.tile(days_, len(acc_idx))
        ok = (d >= np.maximum(acc["opened_day"].to_numpy()[a], 0)) & (d < acc["end_day"].to_numpy()[a])
        return a[ok], d[ok]

    def _transactions(self, acc: pd.DataFrame, merchants: pd.DataFrame) -> pd.DataFrame:
        rng, cfg = self.rng, self.cfg
        days = cfg.days
        m = len(acc)
        a_type = acc["account_type"].to_numpy()
        seg = acc["segment"].to_numpy()
        ccy = acc["currency"].to_numpy()
        checking = a_type == "CHECKING"
        dow = (np.arange(days) + (pd.Timestamp(cfg.start).dayofweek)) % 7
        weekday_factor = np.select([dow == 5, dow == 6], [1.25, 0.7], 1.0)
        typical = rng.lognormal(np.log(22), 0.3, m)
        employer = np.array([make_iban("EMPL", i) for i in range(1, 201)])
        payees = np.array([make_iban("PAYE", i) for i in range(1, 3001)])
        parts = []

        def add(a, d, sec, amount, txn_type, direction, channel, merchant=None, cp=None, rule=None):
            k = len(a)
            parts.append(
                pd.DataFrame(
                    {
                        "acc": a,
                        "t": d * DAY + sec,
                        "amount": np.round(np.maximum(amount, 0.5), 2),
                        "txn_type": txn_type,
                        "direction": direction,
                        "channel": channel,
                        "merchant": merchant if merchant is not None else np.full(k, -1),
                        "counterparty": cp if cp is not None else np.full(k, ""),
                        "rule": rule if rule is not None else np.full(k, ""),
                    }
                )
            )

        card_mask = checking | (a_type == "CREDIT_CARD")
        lam = np.where(seg == "BUSINESS", 2.5, np.where(a_type == "CREDIT_CARD", 0.35, 1.0))
        lam = lam * rng.lognormal(0, 0.4, m)
        a, d = self._poisson_events(card_mask, lam[card_mask], acc, weekday_factor)
        pop = merchants["popularity"].to_numpy()
        merch = rng.choice(len(merchants), len(a), p=pop / pop.sum())
        sec = ((6 + 17.9 * rng.beta(2.2, 2.0, len(a))) * 3600).astype(np.int64)
        amt = typical[a] * merchants["amount_mult"].to_numpy()[merch] * rng.lognormal(0, 0.7, len(a))
        channel = rng.choice(["POS", "WEB", "MOBILE"], len(a), p=[0.75, 0.2, 0.05])
        add(a, d, sec, amt, "CARD_PAYMENT", "DR", channel, merchant=merch)

        salaried = np.flatnonzero(checking & (ccy == "EUR") & (seg != "BUSINESS"))
        salary = rng.lognormal(np.where(seg == "PRIVATE", np.log(9000), np.log(3100)), 0.35)
        pay_days = []
        for day in self._monthly_days(25):
            wd = dow[day]
            pay_days.append(day - (wd - 4 if wd > 4 else 0))
        a, d = self._scheduled(salaried, np.array(pay_days), acc)
        add(
            a,
            d,
            6 * 3600 + rng.integers(0, 3600, len(a)),
            salary[a],
            "SALARY",
            "CR",
            "SEPA",
            cp=employer[a % len(employer)],
        )

        renters = salaried[rng.random(len(salaried)) < 0.6]
        rent = rng.lognormal(np.log(1150), 0.25, m)
        a, d = self._scheduled(renters, self._monthly_days(1), acc)
        add(
            a,
            d,
            7 * 3600 + rng.integers(0, 3600, len(a)),
            rent[a],
            "SEPA_TRANSFER_OUT",
            "DR",
            "SEPA",
            cp=payees[a % len(payees)],
        )

        biz = checking & (seg == "BUSINESS")
        a, d = self._poisson_events(biz, np.full(biz.sum(), 1.2), acc)
        add(
            a,
            d,
            rng.integers(8 * 3600, 20 * 3600, len(a)),
            rng.lognormal(np.log(1400), 1.0, len(a)),
            "SEPA_TRANSFER_IN",
            "CR",
            "SEPA",
            cp=rng.choice(payees, len(a)),
        )

        for lam_v, direction, ttype, mean in [
            (0.07, "DR", "SEPA_TRANSFER_OUT", 140),
            (0.04, "CR", "SEPA_TRANSFER_IN", 120),
        ]:
            a, d = self._poisson_events(checking, np.full(checking.sum(), lam_v), acc)
            add(
                a,
                d,
                rng.integers(7 * 3600, 23 * 3600, len(a)),
                rng.lognormal(np.log(mean), 1.1, len(a)),
                ttype,
                direction,
                rng.choice(["MOBILE", "WEB"], len(a), p=[0.7, 0.3]),
                cp=rng.choice(payees, len(a)),
            )

        a, d = self._poisson_events(checking, np.full(checking.sum(), 0.05), acc)
        add(
            a,
            d,
            rng.integers(8 * 3600, 23 * 3600, len(a)),
            rng.choice([20, 50, 100, 150, 200, 300], len(a)),
            "ATM_WITHDRAWAL",
            "DR",
            "ATM",
        )

        a, d = self._poisson_events(checking, np.where(seg[checking] == "BUSINESS", 0.05, 0.003), acc)
        add(
            a,
            d,
            rng.integers(9 * 3600, 17 * 3600, len(a)),
            rng.lognormal(np.where(seg[a] == "BUSINESS", np.log(700), np.log(300)), 0.8),
            "CASH_DEPOSIT",
            "CR",
            "BRANCH",
        )

        month_end = self._monthly_days(last=True)
        a, d = self._scheduled(np.flatnonzero(checking), month_end, acc)
        add(a, d, np.full(len(a), 23 * 3600), np.full(len(a), 2.95), "FEE", "DR", "SYSTEM")
        savings = np.flatnonzero(a_type == "SAVINGS")
        a, d = self._scheduled(savings, month_end, acc)
        add(a, d, np.full(len(a), 23 * 3600 + 1800), rng.lognormal(np.log(6), 1.0, len(a)), "INTEREST", "CR", "SYSTEM")

        primary = acc[acc["primary"]].set_index("customer_id")["account_id"]
        sav = acc[(a_type == "SAVINGS")]
        sav = sav[rng.random(len(sav)) < 0.45]
        chk_idx = acc.index.to_numpy()[acc["account_id"].isin(primary.loc[sav["customer_id"]].to_numpy())]
        chk_by_cust = pd.Series(chk_idx, index=acc.loc[chk_idx, "customer_id"].to_numpy())
        pairs = pd.DataFrame({"sav": sav.index.to_numpy(), "chk": chk_by_cust.loc[sav["customer_id"]].to_numpy()})
        amount = rng.lognormal(np.log(300), 0.6, len(pairs))
        for day in self._monthly_days(26):
            ok = (
                (day >= np.maximum(acc["opened_day"].to_numpy()[pairs["sav"]], 0))
                & (day < acc["end_day"].to_numpy()[pairs["sav"]])
                & (day < acc["end_day"].to_numpy()[pairs["chk"]])
            )
            p, amt, k = pairs[ok], amount[ok], ok.sum()
            sec = np.full(k, 8 * 3600)
            iban = acc["iban"].to_numpy()
            add(p["chk"].to_numpy(), np.full(k, day), sec, amt, "INTERNAL_TRANSFER", "DR", "SYSTEM", cp=iban[p["sav"]])
            add(
                p["sav"].to_numpy(),
                np.full(k, day),
                sec + 1,
                amt,
                "INTERNAL_TRANSFER",
                "CR",
                "SYSTEM",
                cp=iban[p["chk"]],
            )

        full_period = np.flatnonzero(checking & (ccy == "EUR") & (acc["opened_day"] <= 0) & (acc["end_day"] == days))
        for i, acc_i in enumerate(rng.choice(full_period, 12, replace=False)):
            k = rng.integers(3, 6)
            t0 = rng.integers(10, days - 10) * DAY + rng.integers(8 * 3600, 12 * 3600)
            t = t0 + np.sort(rng.integers(0, 20 * 3600, k))
            add(
                np.full(k, acc_i),
                t // DAY,
                t % DAY,
                rng.uniform(9000, 9950, k),
                "CASH_DEPOSIT",
                "CR",
                "BRANCH",
                rule=np.full(k, f"STRUCTURING:{i}"),
            )
        for i, acc_i in enumerate(rng.choice(full_period, 15, replace=False)):
            k = rng.integers(12, 21)
            t0 = rng.integers(10, days - 10) * DAY + rng.integers(9 * 3600, 21 * 3600)
            t = t0 + np.sort(rng.integers(0, 45 * 60, k))
            add(
                np.full(k, acc_i),
                t // DAY,
                t % DAY,
                rng.lognormal(np.log(15), 0.5, k),
                "CARD_PAYMENT",
                "DR",
                "WEB",
                merchant=rng.choice(len(merchants), k),
                rule=np.full(k, f"VELOCITY:{i}"),
            )
        big_ticket = merchants.index[merchants["mcc"].isin(["5732", "4511", "7011"])].to_numpy()
        outliers = rng.choice(full_period, 20, replace=False)
        add(
            outliers,
            rng.integers(60, days - 5, 20),
            rng.integers(10 * 3600, 20 * 3600, 20),
            typical[outliers] * rng.uniform(150, 250, 20),
            "CARD_PAYMENT",
            "DR",
            "POS",
            merchant=rng.choice(big_ticket, 20),
            rule=np.array([f"OUTLIER:{i}" for i in range(20)]),
        )

        tx = pd.concat(parts, ignore_index=True).sort_values("t", kind="stable").reset_index(drop=True)
        tx["txn_id"] = [f"T{i:010d}" for i in range(1, len(tx) + 1)]
        tx["status"] = "POSTED"
        card = (tx["txn_type"] == "CARD_PAYMENT") & (tx["rule"] == "")
        tx.loc[card & (rng.random(len(tx)) < 0.01), "status"] = "DECLINED"
        tx["original_txn_id"] = ""

        to_reverse = tx[card & (tx["status"] == "POSTED") & (rng.random(len(tx)) < 0.003)]
        rev_t = (
            to_reverse["t"].to_numpy()
            + rng.integers(1, 4, len(to_reverse)) * DAY
            + rng.integers(0, 6 * 3600, len(to_reverse))
        )
        keep = (rev_t // DAY < days) & (rev_t // DAY < acc["end_day"].to_numpy()[to_reverse["acc"]])
        rev = to_reverse[keep].copy()
        rev["t"] = rev_t[keep]
        rev["original_txn_id"] = rev["txn_id"]
        rev["txn_type"] = "REVERSAL"
        rev["direction"] = "CR"
        rev["channel"] = "SYSTEM"
        start_id = len(tx) + 1
        rev["txn_id"] = [f"T{i:010d}" for i in range(start_id, start_id + len(rev))]
        tx = pd.concat([tx, rev], ignore_index=True)

        anomalies = tx[tx["rule"] != ""]
        self.anomalies.append(
            pd.DataFrame(
                {
                    "rule": anomalies["rule"].str.split(":").str[0],
                    "case_id": anomalies["rule"],
                    "txn_id": anomalies["txn_id"],
                    "account_id": acc["account_id"].to_numpy()[anomalies["acc"]],
                }
            )
        )
        return self._to_landing_rows(tx, acc, merchants)

    def _to_landing_rows(self, tx: pd.DataFrame, acc: pd.DataFrame, merchants: pd.DataFrame) -> pd.DataFrame:
        rng, cfg = self.rng, self.cfg
        n = len(tx)
        day = (tx["t"] // DAY).to_numpy()
        out = pd.DataFrame(
            {
                "txn_id": tx["txn_id"],
                "account_id": acc["account_id"].to_numpy()[tx["acc"]],
                "txn_ts": self._ts(tx["t"].to_numpy()),
                "amount": tx["amount"].map("{:.2f}".format),
                "currency": acc["currency"].to_numpy()[tx["acc"]],
                "direction": tx["direction"],
                "txn_type": tx["txn_type"],
                "status": tx["status"],
                "channel": tx["channel"],
                "merchant_id": np.where(
                    tx["merchant"] >= 0, merchants["merchant_id"].to_numpy()[tx["merchant"].clip(lower=0)], ""
                ),
                "counterparty_iban": tx["counterparty"],
                "original_txn_id": tx["original_txn_id"],
            }
        )
        out["description"] = np.where(
            tx["merchant"] >= 0,
            merchants["merchant_name"].str.strip().str.upper().to_numpy()[tx["merchant"].clip(lower=0)],
            tx["txn_type"].str.replace("_", " "),
        )
        digital = tx["channel"].isin(["MOBILE", "WEB"]).to_numpy()
        out["device_id"] = np.where(digital, "DEV-" + pd.Series(tx["acc"]).map("{:06x}".format), "")

        branch = (tx["channel"] == "BRANCH").to_numpy() & (day >= cfg.format_drift_day)
        dt = pd.to_datetime(out.loc[branch, "txn_ts"])
        out.loc[branch, "txn_ts"] = dt.dt.strftime("%d/%m/%Y %H:%M:%S")
        out.loc[branch, "amount"] = out.loc[branch, "amount"].str.replace(".", ",", regex=False)

        reversed_ids = set(tx.loc[tx["original_txn_id"] != "", "original_txn_id"])
        clean = (
            (tx["rule"] == "").to_numpy()
            & (tx["original_txn_id"] == "").to_numpy()
            & ~tx["txn_id"].isin(reversed_ids).to_numpy()
        )
        roll = np.where(clean, rng.random(n), 1.0)

        def inject(mask, issue, column, value):
            out.loc[mask, column] = value if not callable(value) else value(out.loc[mask, column])
            self.issues.append(pd.DataFrame({"entity": "transactions", "key": out.loc[mask, "txn_id"], "issue": issue}))

        inject(roll < 0.0003, "INVALID_AMOUNT", "amount", "N/A")
        inject((roll >= 0.0003) & (roll < 0.0006), "NEGATIVE_AMOUNT", "amount", lambda s: "-" + s)
        inject(
            (roll >= 0.0006) & (roll < 0.0009),
            "UNKNOWN_ACCOUNT",
            "account_id",
            lambda s: pd.Series([f"A9{v:06d}" for v in rng.integers(0, 999_999, len(s))], index=s.index),
        )
        inject((roll >= 0.0009) & (roll < 0.001), "MISSING_TIMESTAMP", "txn_ts", "")
        card_rows = (out["txn_type"] == "CARD_PAYMENT").to_numpy()
        inject(card_rows & (roll >= 0.001) & (roll < 0.003), "MISSING_MERCHANT", "merchant_id", "")
        inject((roll >= 0.003) & (roll < 0.013), "LOWERCASE_CURRENCY", "currency", lambda s: s.str.lower())

        out["file_day"] = day + 1
        late = clean & (rng.random(n) < 0.02)
        out.loc[late, "file_day"] = day[late] + rng.integers(2, 7, late.sum())
        self.issues.append(
            pd.DataFrame({"entity": "transactions", "key": out.loc[late, "txn_id"], "issue": "LATE_ARRIVAL"})
        )

        dupes = out[clean & (rng.random(n) < 0.004)].copy()
        dupes["file_day"] += 1
        self.issues.append(pd.DataFrame({"entity": "transactions", "key": dupes["txn_id"], "issue": "DUPLICATE_ROW"}))
        return pd.concat([out, dupes], ignore_index=True)

    def _file_date(self, file_day: int) -> str:
        return str(self.start + np.timedelta64(int(file_day), "D")).replace("-", "")

    def _write_daily(self, df: pd.DataFrame, folder: Path) -> None:
        folder.mkdir(parents=True, exist_ok=True)
        entity = folder.name
        cols = [c for c in df.columns if c not in {"file_day", "popularity", "amount_mult"}]
        for file_day, part in df.groupby("file_day"):
            part[cols].to_csv(
                folder / f"{entity}_{self._file_date(file_day)}.csv.gz",
                index=False,
                compression={"method": "gzip", "compresslevel": 1, "mtime": 0},
            )

    def _write_transactions(self, df: pd.DataFrame, folder: Path) -> None:
        folder.mkdir(parents=True, exist_ok=True)
        for file_day, part in df.groupby("file_day"):
            part = part.drop(columns="file_day")
            if file_day <= self.cfg.schema_drift_day:
                part = part.drop(columns="device_id")
            part.to_csv(
                folder / f"transactions_{self._file_date(file_day)}.csv.gz",
                index=False,
                compression={"method": "gzip", "compresslevel": 1, "mtime": 0},
            )
