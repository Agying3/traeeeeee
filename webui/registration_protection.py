"""Service-side admission, budget reservations and recovery for SMS workers."""
import asyncio
import math
import sqlite3
import time
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from contextlib import contextmanager


def cents(value):
    number = Decimal(str(value))
    if not number.is_finite() or number < 0:
        raise ValueError("金额须为非负有限数")
    return int((number * 100).quantize(Decimal("1"), rounding=ROUND_HALF_UP))


class RegistrationProtection:
    def __init__(self, directory, config, summary, log, credentials, proxy_indices):
        self.directory = Path(directory)
        self.config, self.summary, self.log = config, summary, log
        self.credentials, self.proxy_indices = credentials, proxy_indices
        self.lock = asyncio.Lock()
        self.cached_balance = None
        self.last_snapshot = 0

    @contextmanager
    def _db(self):
        self.directory.mkdir(parents=True, exist_ok=True)
        db = sqlite3.connect(self.directory / "registration-budget.sqlite3", timeout=15)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("PRAGMA synchronous=FULL")
        db.execute("CREATE TABLE IF NOT EXISTS reservations (attempt_id TEXT PRIMARY KEY, "
                   "batch_id TEXT, price INTEGER NOT NULL, state TEXT NOT NULL, updated REAL, "
                   "receipt_key TEXT DEFAULT '')")
        db.execute("CREATE TABLE IF NOT EXISTS adjustments (id INTEGER PRIMARY KEY, amount INTEGER, "
                   "note TEXT, at REAL)")
        try:
            yield db
            db.commit()
        except BaseException:
            db.rollback()
            raise
        finally:
            db.close()

    def ledger(self):
        from haozhu_ledger import Ledger
        return Ledger(self.directory / "haozhu-ledger.sqlite3")

    def budget(self):
        with self._db() as db:
            rows = db.execute("SELECT state, SUM(price) AS amount FROM reservations GROUP BY state").fetchall()
            grouped = {r["state"]: r["amount"] or 0 for r in rows}
            extra = db.execute("SELECT COALESCE(SUM(amount),0) FROM adjustments").fetchone()[0]
        limit = cents(self.config().get("budget_limit", 0)) + extra
        spent = grouped.get("estimated", 0)
        reserved = grouped.get("reserved", 0) + grouped.get("uncertain", 0)
        return {"enabled": limit > 0, "limit": limit / 100, "estimated_spent": spent / 100,
                "reserved": reserved / 100, "uncertain": grouped.get("uncertain", 0) / 100,
                "available": (limit - spent - reserved) / 100 if limit > 0 else None,
                "basis": "收码证据按入场单价估算；非平台消费流水"}

    def add_budget(self, amount, note):
        value = cents(amount)
        if value <= 0:
            raise ValueError("增加额度须大于零")
        with self._db() as db:
            db.execute("INSERT INTO adjustments(amount,note,at) VALUES(?,?,?)",
                       (value, str(note)[:200], time.time()))
        self.ledger().record("budget_added", amount=value / 100, note=str(note)[:200])
        return self.budget()

    async def snapshot(self, reason, batch_id=""):
        result = await asyncio.to_thread(self.summary)
        if not result or result.get("balance") is None:
            return None
        balance = float(result["balance"])
        if not math.isfinite(balance) or balance < 0:
            return None
        self.cached_balance, self.last_snapshot = balance, time.time()
        self.ledger().record("balance_snapshot", balance=balance, reason=reason,
                             batch_id=batch_id, platform_num_raw=result.get("num_raw", result.get("occupied")),
                             account=self.credentials().get("user", ""))
        return balance

    async def recover(self, exclude=()):
        from haozhu_sms import reclaim_held
        ledger = self.ledger()
        held = ledger.held()
        if not held:
            return []
        results = []
        for row in held:
            if row["attempt_id"] in exclude:
                continue
            result = await asyncio.to_thread(reclaim_held, attempt_id=row["attempt_id"],
                       run_id=row.get("run_id", ""), ledger=ledger,
                       cli_creds=self.credentials(), exclude_attempt_ids=tuple(exclude), recovery=True)
            results.extend(result)
        return results

    def reconcile_budget(self, attempt_id, received, note):
        note = str(note).strip()
        if not note:
            raise ValueError("请填写核对依据")
        row = self.ledger().get_attempt(attempt_id) or {}
        if row.get("held"):
            raise ValueError("号码仍在占用，先完成清理")
        evidence = self._evidence(attempt_id, row)
        if not received and evidence.get("code_received"):
            raise ValueError("已有收码证据，保留收码估算")
        status = "estimated" if received else "released"
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            reservation = db.execute("SELECT state FROM reservations WHERE attempt_id=?", (attempt_id,)).fetchone()
            if reservation is None or reservation["state"] not in ("uncertain", "reserved"):
                raise ValueError("该尝试没有待核对预算")
            receipt = evidence["receipt_key"]
            if received and db.execute("SELECT 1 FROM reservations WHERE receipt_key=? AND state='estimated' "
                                       "AND attempt_id<>?", (receipt, attempt_id)).fetchone():
                status = "released"
            db.execute("UPDATE reservations SET state=?,receipt_key=?,updated=? WHERE attempt_id=?",
                       (status, receipt, time.time(), attempt_id))
        self.ledger().record("budget_reconciled", attempt_id=attempt_id,
                             received=received, note=note[:200], basis="operator evidence")
        return {**self.budget(), "budget_state": status}

    async def preflight(self):
        async with self.lock:
            try:
                await self.recover()
                self.reconcile_reservations()
                if self.ledger().held():
                    return "遗留号码尚未完成清理，请先恢复清理并核对"
                if self.config().get("min_balance", 1.1) > 0:
                    balance = await self.snapshot("before_batch")
                    if balance is None:
                        return "余额查询失败，暂停派发；检查豪猪连接后重新启动"
                    if balance < float(self.config().get("min_balance", 1.1)):
                        return "豪猪余额低于保护下限"
                return ""
            except Exception as error:
                return "启动恢复失败: " + type(error).__name__

    async def before_launch(self, task):
        async with self.lock:
            cfg = self.config()
            price = cents(task.get("run_options", {}).get("sms_unit_price", cfg.get("sms_unit_price", 1.1)))
            budget = self.budget()
            if budget["enabled"] and cents(max(0, budget["available"])) < price:
                return "预算可用额度不足，暂停派发；补充预算后重新启动"
            if float(cfg.get("min_balance", 1.1)) > 0:
                balance = await self.snapshot("before_attempt", task.get("batch_id", ""))
                if balance is None:
                    return "余额查询失败，暂停派发"
                with self._db() as db:
                    reserved = db.execute("SELECT COALESCE(SUM(price),0) FROM reservations "
                                          "WHERE state IN ('reserved','uncertain')").fetchone()[0]
                if cents(balance) - reserved < max(price, cents(cfg.get("min_balance", 1.1))):
                    return "余额扣除在途预留后不足，暂停派发并等待在途任务收尾"
            with self._db() as db:
                db.execute("INSERT INTO reservations(attempt_id,batch_id,price,state,updated) VALUES(?,?,?,?,?)",
                           (task["attempt_id"], task.get("batch_id", ""), price, "reserved", time.time()))
            self.ledger().record("admission_reserved", attempt_id=task["attempt_id"],
                                 batch_id=task.get("batch_id", ""), price=price / 100,
                                 run_id=task["run_id"], proxy=task.get("proxy"))
            return ""

    def _evidence(self, attempt_id, row):
        ledger = self.ledger()
        evidence = {**(ledger.get_attempt(attempt_id) or {}), **(row or {})}
        visited, platform, manual, operator, root = set(), False, False, False, attempt_id
        current = attempt_id
        while current and current not in visited:
            visited.add(current)
            record = ledger.get_attempt(current) or {}
            root = current
            platform |= record.get("code_source") == "platform"
            manual |= record.get("code_source") == "manual"
            flags = ledger.receipt_evidence(current)
            platform |= flags["platform"]
            manual |= flags["manual"]
            operator |= flags["operator"]
            current = record.get("original_attempt_id", "")
        if platform or operator:
            evidence["code_source"] = "platform" if platform else "operator"
            evidence["code_received"] = True
        elif manual:
            evidence["code_source"] = "manual"
        # {意图：同一次号码占用的复用沿原尝试去重，短信轮询次数不增加预算消费。}
        # {意图：人工核对与平台证据共用此键，避免复用再次估算。}
        evidence["receipt_key"] = "|".join(str(evidence.get(k, "")) for k in
                                            ("account_fingerprint", "sid", "phone")) + "|" + root
        return evidence

    def _settle(self, attempt_id, row, interrupted=False, spawned=True):
        row = self._evidence(attempt_id, row)
        source = row.get("code_source") or row.get("source") or ""
        evidence = bool(row.get("code_received") or source == "platform" or row.get("receipt_count"))
        sent = bool(row.get("sms_sent") or row.get("ticket_received"))
        uncertain = bool(interrupted and spawned or row.get("held") or row.get("state") == "acquiring" or
                         source == "manual" or row.get("charge_status") == "unknown")
        status = "estimated" if evidence and source != "manual" else "uncertain" if uncertain or sent else "released"
        # {目标：预算预留不冒充账单；未决收码保留额度，直到有明确证据或人工核对。}
        receipt = str(row.get("receipt_key") or row.get("receipt_hash") or "")
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            if status == "estimated" and receipt and db.execute(
                    "SELECT 1 FROM reservations WHERE receipt_key=? AND state='estimated' AND attempt_id<>?",
                    (receipt, attempt_id)).fetchone():
                status = "released"
            db.execute("UPDATE reservations SET state=?,receipt_key=?,updated=? WHERE attempt_id=?",
                       (status, receipt, time.time(), attempt_id))
        return status

    def reconcile_reservations(self):
        with self._db() as db:
            ids = [r[0] for r in db.execute("SELECT attempt_id FROM reservations WHERE state IN ('reserved','uncertain')")]
        for aid in ids:
            row = self.ledger().get_attempt(aid)
            # A dead worker may have sent an SMS before persisting a receipt.
            self._settle(aid, row or {}, interrupted=True, spawned=True)

    async def after_exit(self, task, interrupted=False, spawned=True):
        from haozhu_sms import reclaim_held
        aid = task["attempt_id"]
        ledger = self.ledger()
        row = ledger.get_attempt(aid)
        if row and row.get("held"):
            await asyncio.to_thread(reclaim_held, attempt_id=aid, run_id=task["run_id"],
                                    ledger=ledger, cli_creds=self.credentials(), recovery=True)
            row = ledger.get_attempt(aid) or row
        row = row or {}
        state = self._settle(aid, row, interrupted, spawned)
        with self._db() as db:
            reservation = db.execute("SELECT price FROM reservations WHERE attempt_id=?", (aid,)).fetchone()
        amount = (reservation[0] if reservation else 0) / 100
        details = {"budget_state": state,
                   "estimated_cost": amount if state == "estimated" else 0,
                   "uncertain_cost": amount if state in ("uncertain", "reserved") else 0}
        for key in ("phone", "code_source", "error_category", "held", "credential_saved"):
            if row.get(key) is not None:
                details[key] = row[key]
        if interrupted or row.get("held") or state == "uncertain":
            details.update(status="reconciliation_unknown", stage="号码清理或收码计费待核对")
        if row.get("credential_saved"):
            details.update(status="ok", cred=True, stage="凭证落袋")
        return details

    async def batch_end(self, batch_id):
        if self.config().get("min_balance", 1.1) > 0:
            await self.snapshot("after_batch", batch_id)
        self.ledger().record("batch_finished", batch_id=batch_id, budget=self.budget())

    def overview(self):
        ledger = self.ledger()
        return {"ledger": ledger.overview(), "budget": self.budget(), "held": ledger.held(),
                "unresolved": self.unresolved(),
                "balance": self.cached_balance, "balance_updated": self.last_snapshot,
                "note": "平台 num 字段语义待核实；占号、收码证据和预算估算分别统计"}

    def reservation_state(self, attempt_id):
        with self._db() as db:
            row = db.execute("SELECT state FROM reservations WHERE attempt_id=?", (attempt_id,)).fetchone()
        return row[0] if row else ""

    def unresolved(self):
        with self._db() as db:
            return [dict(r) for r in db.execute("SELECT attempt_id,batch_id,price,state,updated FROM reservations "
                                               "WHERE state='uncertain' ORDER BY updated DESC")]
