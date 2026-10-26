"""Seed Lumen Bank (a fictional bank) with realistic synthetic data.

Creates ~1,000 customers, ~1,800 accounts and ~15,000 transactions over the last 60 days,
following simple real-world patterns: salaries land on the 1st, EMIs on the 5th, rent on the
3rd, richer segments spend more and travel more, and some customers have expired KYC.

Usage:
    python seed_bank.py            # insert into Oracle (refuses if data already exists)
    python seed_bank.py --dry-run  # only print what would be generated
"""
import datetime as dt
import os
import random
import sys
from collections import Counter

random.seed(42)  # same data every run - reproducible

N_CUSTOMERS = 1000
DAYS = 60
TODAY = dt.date.today()
START = TODAY - dt.timedelta(days=DAYS)

FIRST = ["Aarav", "Ananya", "Vihaan", "Diya", "Arjun", "Ishita", "Kabir", "Meera", "Rohan", "Saanvi",
         "Aditya", "Kavya", "Vivaan", "Riya", "Krishna", "Tara", "Siddharth", "Nisha", "Rahul", "Pooja",
         "Yash", "Sneha", "Dev", "Aisha", "Nikhil", "Priya", "Karan", "Zoya", "Aman", "Shreya",
         "Farhan", "Lakshmi", "Varun", "Neha", "Harsh", "Anjali", "Imran", "Divya", "Manav", "Sara"]
LAST = ["Sharma", "Iyer", "Reddy", "Nair", "Gupta", "Mehta", "Khan", "Singh", "Patel", "Das",
        "Menon", "Joshi", "Rao", "Bose", "Kapoor", "Verma", "Pillai", "Chopra", "Banerjee", "Kulkarni",
        "Agarwal", "Fernandes", "Malhotra", "Saxena", "Chatterjee", "Bhat", "Ali", "Mishra", "Sinha", "Desai"]
CITIES = ["Mumbai", "Delhi", "Bengaluru", "Hyderabad", "Chennai", "Pune", "Kolkata", "Ahmedabad", "Jaipur", "Kochi"]
CITY_W = [18, 17, 16, 11, 9, 9, 7, 5, 4, 4]

SEGMENTS = {  # weight, income range (Rs/year), daily spend-txn rate, chance of travel spend
    "MASS":     (60, (300_000, 900_000),     0.12, 0.03),
    "AFFLUENT": (30, (900_000, 2_500_000),   0.25, 0.10),
    "PREMIUM":  (10, (2_500_000, 8_000_000), 0.40, 0.22),
}
MERCHANTS = {
    "GROCERY":   (["FreshMart", "DailyBasket", "GreenGrocer", "QuickKart"], (200, 3500)),
    "DINING":    (["Spice Route", "Chai Point Cafe", "Biryani House", "Dosa Corner"], (150, 4000)),
    "FUEL":      (["IndiFuel", "HighwayFuels", "CityPetro"], (500, 4000)),
    "SHOPPING":  (["StyleHub", "GadgetZone", "HomeNest", "BookBarn"], (400, 15000)),
    "UTILITIES": (["PowerGrid Bill", "AirWave Mobile", "FiberNet"], (300, 3000)),
    "TRAVEL":    (["SkyHop Airlines", "RailYatra", "StayInn Hotels", "CabGo"], (1500, 45000)),
}
EVERYDAY = ["GROCERY", "DINING", "FUEL", "SHOPPING", "UTILITIES"]
EVERYDAY_W = [35, 25, 12, 18, 10]


def pick(weighted: dict):
    keys = list(weighted)
    return random.choices(keys, weights=[weighted[k][0] for k in keys])[0]


def build():
    customers, accounts, txns = [], [], []
    acc_id, txn_id = 100_000, 1_000_000

    def add_txn(account, day, amount, direction, channel, category, merchant, city):
        nonlocal txn_id
        txn_id += 1
        ts = dt.datetime.combine(day, dt.time(random.randint(7, 22), random.randint(0, 59), random.randint(0, 59)))
        txns.append((txn_id, account, ts, round(amount, 2), direction, channel, category, merchant, city))

    for cid in range(1, N_CUSTOMERS + 1):
        first, last = random.choice(FIRST), random.choice(LAST)
        city = random.choices(CITIES, weights=CITY_W)[0]
        seg = pick(SEGMENTS)
        _, (lo, hi), spend_rate, travel_p = SEGMENTS[seg]
        income = round(random.uniform(lo, hi), -3)
        kyc = random.choices(["VERIFIED", "PENDING", "EXPIRED"], weights=[90, 6, 4])[0]
        risk = random.choices(["LOW", "MEDIUM", "HIGH"], weights=[75, 20, 5])[0]
        joined = START - dt.timedelta(days=random.randint(30, 2500))
        email = f"{first}.{last}{cid}@example.com".lower()
        customers.append((cid, f"{first} {last}", email, city, seg, income, kyc, risk, joined))

        # --- accounts ---
        my_accounts = {}
        def open_account(atype, balance, status="ACTIVE"):
            nonlocal acc_id
            acc_id += 1
            opened = joined + dt.timedelta(days=random.randint(0, 200))
            accounts.append((acc_id, cid, atype, round(balance, 2), status, opened))
            my_accounts[atype] = acc_id

        savings_status = "FROZEN" if kyc == "EXPIRED" else random.choices(["ACTIVE", "DORMANT"], weights=[97, 3])[0]
        open_account("SAVINGS", random.uniform(0.05, 0.6) * income, savings_status)
        richer = seg != "MASS"
        if random.random() < (0.7 if richer else 0.25):
            open_account("CREDIT_CARD", -random.uniform(0, 0.08) * income)
        if random.random() < (0.35 if richer else 0.08):
            open_account("FIXED_DEPOSIT", random.uniform(0.2, 1.5) * income)
        if random.random() < (0.25 if richer else 0.05):
            open_account("HOME_LOAN", -random.uniform(1.5, 6) * income)
        if random.random() < 0.05:
            open_account("CURRENT", random.uniform(0.05, 0.5) * income)

        if savings_status != "ACTIVE":
            continue  # frozen / dormant customers have no recent activity
        sav = my_accounts["SAVINGS"]
        card = my_accounts.get("CREDIT_CARD")
        salaried = random.random() < 0.85
        pays_rent = random.random() < 0.4
        emi = round(income / 12 * random.uniform(0.15, 0.35), -2) if "HOME_LOAN" in my_accounts else 0

        for d in range(DAYS):
            day = START + dt.timedelta(days=d + 1)
            if day.day == 1 and salaried:
                add_txn(sav, day, round(income / 12, -2), "CR", "NEFT", "SALARY", "Employer Payroll", city)
            if day.day == 3 and pays_rent:
                add_txn(sav, day, round(income / 12 * random.uniform(0.2, 0.35), -2), "DR", "UPI", "RENT", "Landlord", city)
            if day.day == 5 and emi:
                add_txn(sav, day, emi, "DR", "NEFT", "EMI", "Lumen Home Loan", city)
            # everyday spending
            n = (random.random() < spend_rate) + (random.random() < spend_rate / 3)
            for _ in range(n):
                if random.random() < travel_p:
                    cat = "TRAVEL"
                    where = random.choice([c for c in CITIES if c != city])
                else:
                    cat = random.choices(EVERYDAY, weights=EVERYDAY_W)[0]
                    where = city
                names, (a, b) = MERCHANTS[cat]
                amount = random.uniform(a, b) * (1.6 if seg == "PREMIUM" else 1.0)
                use_card = card and random.random() < 0.55
                add_txn(card if use_card else sav, day, amount, "DR",
                        "CARD" if use_card else "UPI", cat, random.choice(names), where)
            if random.random() < 0.02:
                add_txn(sav, day, random.choice([2000, 5000, 10000]), "DR", "ATM", "CASH", "Lumen ATM", city)

    return customers, accounts, txns


def summary(customers, accounts, txns):
    print(f"customers   : {len(customers):,}")
    print(f"accounts    : {len(accounts):,}  {dict(Counter(a[2] for a in accounts))}")
    print(f"transactions: {len(txns):,}  {dict(Counter(t[6] for t in txns).most_common(6))}")
    print(f"segments    : {dict(Counter(c[4] for c in customers))}")
    print(f"kyc         : {dict(Counter(c[6] for c in customers))}")


def main():
    customers, accounts, txns = build()
    summary(customers, accounts, txns)
    if "--dry-run" in sys.argv:
        return

    import oracledb
    from dotenv import load_dotenv
    load_dotenv()
    conn = oracledb.connect(user="bank", password=os.environ["BANK_PASSWORD"],
                            dsn="127.0.0.1:1521/FREEPDB1")
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM customer")
    if cur.fetchone()[0] > 0:
        sys.exit("BANK.CUSTOMER already has data - refusing to seed twice. "
                 "For a fresh start: docker compose down -v, then up -d.")

    cur.executemany("INSERT INTO customer VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9)", customers)
    cur.executemany("INSERT INTO accounts VALUES (:1,:2,:3,:4,:5,:6)", accounts)
    for i in range(0, len(txns), 5000):
        cur.executemany("INSERT INTO transactions VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9)", txns[i:i + 5000])
    conn.commit()
    print("Seeded Oracle. Next: python register_connector.py")


if __name__ == "__main__":
    main()
