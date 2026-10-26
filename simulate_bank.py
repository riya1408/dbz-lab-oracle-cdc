"""Lumen Bank traffic simulator (a fictional bank, synthetic data).

Writes realistic activity into Oracle, so the CDC pipeline has something live to carry.
Every payment is ONE Oracle transaction touching TWO tables: a row in TRANSACTIONS and a
balance update in ACCOUNTS - so related events reach Kafka on two different topics.

Scenarios
  python simulate_bank.py normal --seconds 60       everyday UPI/card payments, ~2 per second
  python simulate_bank.py salary-day                salaries for every salaried customer (a burst)
  python simulate_bank.py fraud [--customer 42]     6 card payments in 4 cities within seconds
  python simulate_bank.py kyc-expire --customer 42  KYC expires -> savings account frozen
  python simulate_bank.py move --customer 42 --city Pune
"""
import argparse
import datetime as dt
import os
import random
import time

import oracledb
from dotenv import load_dotenv

CITIES = ["Mumbai", "Delhi", "Bengaluru", "Hyderabad", "Chennai", "Pune", "Kolkata", "Ahmedabad", "Jaipur", "Kochi"]
EVERYDAY = {"GROCERY": (["FreshMart", "DailyBasket", "QuickKart"], (200, 3500)),
            "DINING": (["Spice Route", "Chai Point Cafe", "Biryani House"], (150, 4000)),
            "FUEL": (["IndiFuel", "CityPetro"], (500, 4000)),
            "SHOPPING": (["StyleHub", "GadgetZone", "HomeNest"], (400, 15000))}


def connect():
    load_dotenv()
    return oracledb.connect(user="bank", password=os.environ["BANK_PASSWORD"], dsn="127.0.0.1:1521/FREEPDB1")


def now_utc():
    """Timestamps are stored in UTC, so the consumer and dashboard agree on 'now'."""
    return dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)


class Bank:
    def __init__(self, conn):
        self.conn, self.cur = conn, conn.cursor()
        self.cur.execute("SELECT NVL(MAX(txn_id), 0) FROM transactions")
        self.txn_id = self.cur.fetchone()[0]

    def pay(self, account_id, amount, direction, channel, category, merchant, city, commit=True):
        """Insert a transaction AND move the account balance - one Oracle transaction."""
        self.txn_id += 1
        amount = round(amount, 2)
        self.cur.execute("INSERT INTO transactions VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9)",
                         [self.txn_id, account_id, now_utc(), amount, direction, channel, category, merchant, city])
        delta = amount if direction == "CR" else -amount
        self.cur.execute("UPDATE accounts SET balance = balance + :1 WHERE account_id = :2", [delta, account_id])
        if commit:
            self.conn.commit()
        return self.txn_id

    def active_accounts(self):
        self.cur.execute("""
            SELECT a.account_id, a.customer_id, a.account_type, c.city
            FROM accounts a JOIN customer c ON c.customer_id = a.customer_id
            WHERE a.status = 'ACTIVE' AND c.kyc_status = 'VERIFIED'
              AND a.account_type IN ('SAVINGS', 'CREDIT_CARD')""")
        return self.cur.fetchall()


def normal(bank, seconds):
    accounts = bank.active_accounts()
    end, n = time.time() + seconds, 0
    print(f"Normal activity for {seconds}s across {len(accounts)} accounts...")
    while time.time() < end:
        account_id, customer_id, atype, city = random.choice(accounts)
        cat = random.choice(list(EVERYDAY))
        names, (lo, hi) = EVERYDAY[cat]
        channel = "CARD" if atype == "CREDIT_CARD" else "UPI"
        tid = bank.pay(account_id, random.uniform(lo, hi), "DR", channel, cat, random.choice(names), city)
        n += 1
        print(f"  txn {tid}: customer {customer_id} paid {cat.lower()} via {channel} in {city}")
        time.sleep(random.uniform(0.2, 0.8))
    print(f"Done: {n} payments.")


def salary_day(bank):
    bank.cur.execute("""
        SELECT a.account_id, c.annual_income, c.city
        FROM accounts a JOIN customer c ON c.customer_id = a.customer_id
        WHERE a.account_type = 'SAVINGS' AND a.status = 'ACTIVE'""")
    rows = bank.cur.fetchall()
    started = time.time()
    print(f"Salary day: crediting {len(rows)} salaries...")
    for i, (account_id, income, city) in enumerate(rows, 1):
        bank.pay(account_id, round(float(income) / 12, -2), "CR", "NEFT", "SALARY", "Employer Payroll",
                 city, commit=False)
        if i % 200 == 0:
            bank.conn.commit()
            print(f"  {i} salaries committed")
    bank.conn.commit()
    print(f"Done: {len(rows)} salaries ({2 * len(rows)} row changes) in {time.time() - started:.1f}s.")


def fraud(bank, customer_id):
    if customer_id is None:
        bank.cur.execute("""SELECT customer_id FROM accounts WHERE account_type = 'CREDIT_CARD'
                            AND status = 'ACTIVE' ORDER BY DBMS_RANDOM.VALUE FETCH FIRST 1 ROWS ONLY""")
        customer_id = bank.cur.fetchone()[0]
    bank.cur.execute("""SELECT a.account_id, a.account_type, c.name, c.city
                        FROM accounts a JOIN customer c ON c.customer_id = a.customer_id
                        WHERE a.customer_id = :1 AND a.status = 'ACTIVE'
                        ORDER BY CASE a.account_type WHEN 'CREDIT_CARD' THEN 0 ELSE 1 END
                        FETCH FIRST 1 ROWS ONLY""", [customer_id])
    account_id, atype, name, home = bank.cur.fetchone()
    cities = random.sample([c for c in CITIES if c != home], 4)
    print(f"FRAUD SCENARIO on customer {customer_id} ({name}, lives in {home}), {atype} {account_id}")
    for i in range(6):
        city = cities[i % 4]
        merchant = random.choice(["GadgetZone", "LuxeJewels", "QuickPay Wallet", "ElectroMart"])
        tid = bank.pay(account_id, random.uniform(15000, 60000), "DR", "CARD", "SHOPPING", merchant, city)
        print(f"  txn {tid}: card used at {merchant} in {city}")
        time.sleep(2)
    print("Done. Watch customer", customer_id, "for an UNUSUAL_CARD_ACTIVITY alert.")


def kyc_expire(bank, customer_id):
    bank.cur.execute("UPDATE customer SET kyc_status = 'EXPIRED' WHERE customer_id = :1", [customer_id])
    bank.cur.execute("UPDATE accounts SET status = 'FROZEN' WHERE customer_id = :1 AND account_type = 'SAVINGS'",
                     [customer_id])
    bank.conn.commit()
    print(f"Customer {customer_id}: KYC expired, savings account frozen (one transaction, two tables).")


def move(bank, customer_id, city):
    bank.cur.execute("UPDATE customer SET city = :1 WHERE customer_id = :2", [city, customer_id])
    bank.conn.commit()
    print(f"Customer {customer_id} moved to {city}.")


def main():
    p = argparse.ArgumentParser(description="Lumen Bank (fictional) traffic simulator")
    p.add_argument("scenario", choices=["normal", "salary-day", "fraud", "kyc-expire", "move"])
    p.add_argument("--seconds", type=int, default=60)
    p.add_argument("--customer", type=int)
    p.add_argument("--city")
    args = p.parse_args()
    if args.scenario in ("kyc-expire", "move") and args.customer is None:
        p.error("--customer is required for this scenario")
    if args.scenario == "move" and not args.city:
        p.error("--city is required for move")

    bank = Bank(connect())
    if args.scenario == "normal":
        normal(bank, args.seconds)
    elif args.scenario == "salary-day":
        salary_day(bank)
    elif args.scenario == "fraud":
        fraud(bank, args.customer)
    elif args.scenario == "kyc-expire":
        kyc_expire(bank, args.customer)
    else:
        move(bank, args.customer, args.city)


if __name__ == "__main__":
    main()
