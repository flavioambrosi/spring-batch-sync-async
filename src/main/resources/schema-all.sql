DROP TABLE cashback IF EXISTS;

DROP TABLE account IF EXISTS;

CREATE TABLE cashback (
    cashback_id BIGINT IDENTITY NOT NULL PRIMARY KEY,
    phoneNumber VARCHAR(30),
    cardNumber VARCHAR(30),
    amount NUMERIC(10,2),
    currency VARCHAR(3),
    status VARCHAR(15)
);

CREATE TABLE account (
    account_id BIGINT IDENTITY NOT NULL PRIMARY KEY,
    totalTransactions NUMERIC(5),
    totalCredit NUMERIC(10,2),
    balance NUMERIC(10,2)
);