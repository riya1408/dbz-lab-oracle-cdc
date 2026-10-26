#!/bin/bash
# Migration 02: Lumen Bank (fictional) schema - customers, accounts, transactions.
# Runs on every container start, after 01. Idempotent: skips if BANK.CUSTOMER exists.
# Tables start empty; seed_bank.py fills them.
(
set -e

EXISTS=$(sqlplus -s / as sysdba <<'SQL'
SET HEADING OFF FEEDBACK OFF PAGESIZE 0
ALTER SESSION SET CONTAINER = FREEPDB1;
SELECT COUNT(*) FROM dba_tables WHERE owner = 'BANK' AND table_name = 'CUSTOMER';
EXIT;
SQL
)
if [ "$(echo "$EXISTS" | tr -d '[:space:]')" != "0" ]; then
  echo ">>> [migration-02] Bank schema already exists - skipping."
  exit 0
fi

echo ">>> [migration-02] Creating Lumen Bank schema (CUSTOMER, ACCOUNTS, TRANSACTIONS)"
sqlplus -s / as sysdba <<'SQL'
WHENEVER SQLERROR EXIT FAILURE
ALTER SESSION SET CONTAINER = FREEPDB1;

CREATE TABLE bank.customer (
  customer_id    NUMBER        PRIMARY KEY,
  name           VARCHAR2(100) NOT NULL,
  email          VARCHAR2(120),
  city           VARCHAR2(50),
  segment        VARCHAR2(20),          -- MASS / AFFLUENT / PREMIUM
  annual_income  NUMBER,
  kyc_status     VARCHAR2(12),          -- VERIFIED / PENDING / EXPIRED
  risk_tier      VARCHAR2(8),           -- LOW / MEDIUM / HIGH
  joined_on      DATE
);

CREATE TABLE bank.accounts (
  account_id     NUMBER        PRIMARY KEY,
  customer_id    NUMBER        NOT NULL REFERENCES bank.customer(customer_id),
  account_type   VARCHAR2(20),          -- SAVINGS / CURRENT / CREDIT_CARD / FIXED_DEPOSIT / HOME_LOAN
  balance        NUMBER,
  status         VARCHAR2(10),          -- ACTIVE / DORMANT / FROZEN / CLOSED
  opened_on      DATE
);
CREATE INDEX bank.ix_accounts_customer ON bank.accounts(customer_id);

CREATE TABLE bank.transactions (
  txn_id         NUMBER        PRIMARY KEY,
  account_id     NUMBER        NOT NULL REFERENCES bank.accounts(account_id),
  txn_ts         TIMESTAMP     NOT NULL,
  amount         NUMBER        NOT NULL,
  direction      VARCHAR2(2)   NOT NULL,  -- CR (money in) / DR (money out)
  channel        VARCHAR2(10),            -- UPI / CARD / NEFT / ATM / IMPS
  category       VARCHAR2(20),            -- GROCERY / DINING / FUEL / TRAVEL / SALARY / RENT / EMI ...
  merchant       VARCHAR2(60),
  city           VARCHAR2(50)
);
CREATE INDEX bank.ix_txn_account ON bank.transactions(account_id);

ALTER TABLE bank.customer     ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
ALTER TABLE bank.accounts     ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
ALTER TABLE bank.transactions ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
EXIT;
SQL
echo ">>> [migration-02] Done."
)
