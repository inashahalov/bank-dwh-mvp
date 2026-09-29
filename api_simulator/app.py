"""
Симулятор банковского API для трёх условных источников: vtb, sber, rshb.
Данные детерминированы (seed = hash(bank, date, entity)), поэтому повторный
запрос за ту же дату отдаёт тот же набор транзакций — это важно для
идемпотентности инкрементальной загрузки в Airflow/Spark.

Эндпоинты сознательно бедны: без пагинации по курсору, без rate-limit,
без версии API. Это уместно для симулятора, но было бы недопустимо
для реального интеграционного проекта — см. раздел "Что не MVP" в README.
"""
import hashlib
import random
from datetime import date, datetime, timedelta

from fastapi import FastAPI, Query
from faker import Faker

app = FastAPI(title="Bank API Simulator")

BANKS = ["vtb", "sber", "rshb"]
SEGMENTS = ["retail", "premium", "private"]
PRODUCT_TYPES = ["card", "deposit", "loan", "current"]
TX_TYPES = ["purchase", "transfer_in", "transfer_out", "withdrawal", "fee"]
MERCHANT_CATEGORIES = ["grocery", "transport", "utilities", "ecom", "restaurants", "cash", "other"]
KYC_LEVELS = ["low", "low", "low", "medium", "high"]  # смещено к low для правдоподобия


def _seed(*parts) -> int:
    h = hashlib.sha256("|".join(str(p) for p in parts).encode()).hexdigest()
    return int(h[:8], 16)


def _clients_for_bank(bank: str, n: int):
    rnd = random.Random(_seed("clients", bank))
    fake = Faker("ru_RU")
    fake.seed_instance(_seed("clients", bank))
    clients = []
    for i in range(n):
        client_id = _seed(bank, "client", i) % 10**9
        clients.append({
            "client_id": client_id,
            "full_name": fake.name(),
            "birth_date": fake.date_of_birth(minimum_age=18, maximum_age=80).isoformat(),
            "segment": rnd.choice(SEGMENTS),
            "region": fake.region() if hasattr(fake, "region") else fake.city(),
            "kyc_risk_level": rnd.choice(KYC_LEVELS),
            "source_bank": bank,
        })
    return clients


def _accounts_for_bank(bank: str, n_clients: int, accounts_per_client: int):
    rnd = random.Random(_seed("accounts", bank))
    accounts = []
    for i in range(n_clients):
        client_id = _seed(bank, "client", i) % 10**9
        for j in range(rnd.randint(1, accounts_per_client)):
            account_id = _seed(bank, "account", i, j) % 10**9
            opened = date(2015, 1, 1) + timedelta(days=rnd.randint(0, 4000))
            accounts.append({
                "account_id": account_id,
                "client_id": client_id,
                "product_type": rnd.choice(PRODUCT_TYPES),
                "currency": "RUB",
                "opened_at": opened.isoformat(),
                "is_active": rnd.random() > 0.05,
                "source_bank": bank,
            })
    return accounts


@app.get("/clients")
def get_clients(bank: str = Query(..., pattern="^(vtb|sber|rshb)$"), limit: int = 200):
    return _clients_for_bank(bank, limit)


@app.get("/accounts")
def get_accounts(bank: str = Query(..., pattern="^(vtb|sber|rshb)$"), limit: int = 200):
    return _accounts_for_bank(bank, limit, accounts_per_client=3)


@app.get("/transactions")
def get_transactions(
    bank: str = Query(..., pattern="^(vtb|sber|rshb)$"),
    for_date: str = Query(..., alias="date"),
    limit: int = 500,
):
    """
    Транзакции за конкретный календарный день (батч), детерминированные по
    (bank, for_date). Имитирует ежедневную выгрузку из процессингового ядра.
    """
    accounts = _accounts_for_bank(bank, limit // 3 + 1, accounts_per_client=3)
    rnd = random.Random(_seed("tx", bank, for_date))
    day = datetime.fromisoformat(for_date)
    transactions = []
    for i in range(limit):
        acc = rnd.choice(accounts)
        tx_id = _seed(bank, for_date, "tx", i) % 10**12
        tx_type = rnd.choice(TX_TYPES)
        amount = round(rnd.uniform(50, 150000) * (1 if tx_type != "fee" else 0.05), 2)
        ts = day + timedelta(seconds=rnd.randint(0, 86399))
        transactions.append({
            "transaction_id": tx_id,
            "account_id": acc["account_id"],
            "client_id": acc["client_id"],
            "tx_ts": ts.isoformat(),
            "tx_type": tx_type,
            "amount": amount,
            "currency": "RUB",
            "merchant_category": rnd.choice(MERCHANT_CATEGORIES),
            "is_flagged": rnd.random() < 0.01,  # ~1% простой фрод-флаг
            "source_bank": bank,
        })
    return transactions


@app.get("/health")
def health():
    return {"status": "ok"}
