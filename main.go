package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

type UserId int64

type RawTransactionBalance struct {
	Balance            int64 `db:"balance"`
	TransactionNumSend int64 `db:"transaction_num_send"`
	TransactionNumRecv int64 `db:"transaction_num_recv"`
}

type RawAccountBalance struct {
	TransactionNumRecv int64 `db:"transaction_num_recv"`
	TransactionNumSend int64 `db:"transaction_num_send"`
	Sum                int64 `db:"sum"`
}

const (
	port     = 15432
	username = "postgres"
	password = "postgres"
	dbname   = "bank"
)

type Server struct {
	db *sqlx.DB
}

func NewServer(db *sqlx.DB) *Server {
	return &Server{db: db}
}

func main() {
	db, err := sqlx.Connect(
		"postgres",
		fmt.Sprintf("port=%d user=%s password=%s dbname=%s sslmode=disable", port, username, password, dbname))
	if err != nil {
		fmt.Printf("err %s", err)
	}

	ctx := context.Background()

	server := NewServer(db)
	server.withdraw(ctx, UserId(2), 100)
}

func (s *Server) add(ctx context.Context, id UserId, amount int64) error {
	tx, err := s.db.BeginTxx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return fmt.Errorf("can't start transaction: %w", err)
	}
	rows, err := tx.QueryxContext(ctx, `
		INSERT INTO transactions (id, user_from, user_to, amount, created_at)
			VALUES (gen_random_uuid(), null, $1, $2, NOW());
	`, id, amount)
	if err != nil {
		tx.Rollback() // тут и далее везде в обработке ошибок нужно сделать rollback транзакции и обработать ошибку от этой функции
		return fmt.Errorf("can't run query: %w", err)
	}
	rows.Close() // и обработать все ошибки во всех функциях
	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("can't commit transaction: %w", err)
	}

	return nil
}

func (s *Server) withdraw(ctx context.Context, id UserId, amount int64) error {
	tx, err := s.db.BeginTxx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return fmt.Errorf("can't start transaction: %w", err)
	}

	balance, transactionNumSend, transactionNumRecv, err := getBalance(ctx, tx, id)
	if err != nil {
		return fmt.Errorf("can't get balance: %w", err)
	}
	err = updateBalance(ctx, id, balance, transactionNumSend, transactionNumRecv, tx)
	if err != nil {
		return fmt.Errorf("can't update balance: %w", err)
	}
	if balance < amount {
		tx.Commit()
		return errors.New("balance < amount")
	}

	rows, err := tx.QueryxContext(ctx, `
		INSERT INTO transactions (id, user_from, user_to, amount, created_at)
			VALUES (gen_random_uuid(), $1, null, $2, NOW());
	`, id, amount)
	if err != nil {
		return fmt.Errorf("can't run query: %w", err)
	}
	rows.Close()

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("can't commit transaction: %w", err)
	}

	return nil
}

func (s *Server) transfer(ctx context.Context, fromId UserId, toId UserId, amount int64) error {
	tx, err := s.db.BeginTxx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return fmt.Errorf("can't start transaction: %w", err)
	}

	balance, transactionNumSend, transactionNumRecv, err := getBalance(ctx, tx, fromId)
	if err != nil {
		return fmt.Errorf("can't get balance: %w", err)
	}
	err = updateBalance(ctx, fromId, balance, transactionNumSend, transactionNumRecv, tx)
	if err != nil {
		return fmt.Errorf("can't update balance: %w", err)
	}
	if balance < amount {
		err = tx.Commit()
		return errors.New("balance < amount")
	}

	rows, err := tx.QueryxContext(ctx, `
		INSERT INTO transactions (id, user_from, user_to, amount, created_at)
			VALUES (gen_random_uuid(), $1, $2, $3, NOW())
	`, fromId, toId, amount)
	if err != nil {
		err = tx.Rollback()

		return fmt.Errorf("can't run query: %w", err)
	}
	rows.Close()

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("can't commit transaction: %w", err)
	}

	return nil
}

func (s *Server) balance(ctx context.Context, id UserId) (int64, error) {
	tx, err := s.db.BeginTxx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return 0, fmt.Errorf("can't start transaction: %w", err)
	}

	balance, transactionNumSend, transactionNumRecv, err := getBalance(ctx, tx, id)
	if err != nil {
		err = tx.Rollback()
		return 0, fmt.Errorf("can't get balance: %w", err)
	}

	err = updateBalance(ctx, id, balance, transactionNumSend, transactionNumRecv, tx)
	if err != nil {
		err = tx.Rollback()
		// ...
	}

	err = tx.Commit()
	if err != nil {
		return 0, fmt.Errorf("can't commit transaction: %w", err)
	}

	return balance, nil
}

func getBalance(ctx context.Context, tx *sqlx.Tx, id UserId) (balance int64, transactionNumSend int64, transactionNumRecv int64, err error) {
	rowsAccountBalance := tx.QueryRowxContext(
		ctx,
		`SELECT transaction_num_send, transaction_num_recv, sum FROM accounts WHERE user_id = $1`,
		id)
	accountBalance := &RawAccountBalance{}
	err = rowsAccountBalance.StructScan(accountBalance)
	if err != nil && err != sql.ErrNoRows {
		return 0, 0, 0, fmt.Errorf("can't scan accountBalance: %w", err)
	}

	rowsTransactionsBalance := tx.QueryRowxContext(ctx, `
		SELECT COALESCE(SUM(s), 0) balance, COALESCE(MAX(tn_recv), 0) transaction_num_recv, COALESCE(MAX(tn_send), 0) transaction_num_send FROM (
			SELECT SUM(amount) s, $2 tn_send, MAX(transaction_num) tn_recv FROM transactions 
				WHERE user_to = $1 AND transaction_num > $3
			UNION
			SELECT -1*SUM(amount) s, MAX(transaction_num) tn_send, $3 tn_recv FROM transactions
				WHERE user_from = $1 AND transaction_num > $2
		) sums
	`, id, accountBalance.TransactionNumSend, accountBalance.TransactionNumRecv)
	transactionBalance := &RawTransactionBalance{}
	err = rowsTransactionsBalance.StructScan(transactionBalance)
	if err != nil && err != sql.ErrNoRows {
		return 0, 0, 0, fmt.Errorf("can't scan transactionBalance: %w", err)
	}

	return accountBalance.Sum + transactionBalance.Balance, transactionBalance.TransactionNumSend, transactionBalance.TransactionNumRecv, nil
}

func updateBalance(ctx context.Context, userId UserId, balance int64, transactionNumSend int64, transactionNumRecv int64, tx *sqlx.Tx) error {
	rows, err := tx.QueryxContext(ctx, `INSERT INTO accounts (user_id, transaction_num_send, transaction_num_recv, sum)
			VALUES ($1, $2, $3, $4)
			ON CONFLICT (user_id) DO UPDATE
			SET transaction_num_send = $2,
			    transaction_num_recv = $3,
				sum = $4
	`, userId, transactionNumSend, transactionNumRecv, balance)
	if err != nil {
		return fmt.Errorf("can't run query: %w", err)
	}
	defer rows.Close()

	return nil
}
