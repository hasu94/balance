SELECT 'CREATE DATABASE bank'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'bank')\gexec

create table if not exists accounts
(
    user_id         bigint not null
        primary key,
    transaction_num bigint not null,
    sum             bigint not null
);

alter table accounts
    owner to postgres;

create index user_id_transaction_num_recv_idx
    on accounts (user_id, transaction_num);

create table if not exists transactions
(
    id              uuid not null,
    transaction_num bigserial,
    user_from       bigint,
    user_to         bigint,
    amount          bigint,
    created_at      timestamp,
    primary key (id, transaction_num)
);

alter table transactions
    owner to postgres;

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";