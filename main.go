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

// add Зачисляет средства на счет пользователя. Добавляет в таблицу транзакций строку "пользователь id получил amount копеек"
func (s *Server) add(ctx context.Context, userId UserId, amount int64) error {
	tx, err := s.db.BeginTxx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return fmt.Errorf("can't start transaction: %w", err)
	}
	rows, err := tx.QueryxContext(ctx, `
		INSERT INTO transactions (id, user_from, user_to, amount, created_at)
			VALUES (gen_random_uuid(), null, $1, $2, NOW());
	`, userId, amount)
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

// withdraw Списывает средства со счета пользователя. Вычисляет баланс пользователя, добавляет/обновляет строку в таблице accounts:
// "у пользователя id посчитан баланс для последней транзакции насчисления средств transactionNumRecv, транзакции списания средств transcationNumSend,
// он составляет столько-то копеек". (см. описание функции вычисления баланса) Если баланс пользователя выше, чем amount, то добавляем строку в таблицу transactions
// "у пользователя id списано amount копеек"
func (s *Server) withdraw(ctx context.Context, userId UserId, amount int64) error {
	tx, err := s.db.BeginTxx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return fmt.Errorf("can't start transaction: %w", err)
	}

	balance, transactionNumSend, transactionNumRecv, err := getBalance(ctx, tx, userId)
	if err != nil {
		return fmt.Errorf("can't get balance: %w", err)
	}
	err = updateBalance(ctx, userId, balance, transactionNumSend, transactionNumRecv, tx)
	if err != nil {
		return fmt.Errorf("can't update balance: %w", err)
	}
	if balance < amount {
		tx.Commit()
		return errors.New("balance < amount")
	}

	rows, err := tx.QueryxContext(ctx, `
		INSERT INTO transactions (userId, user_from, user_to, amount, created_at)
			VALUES (gen_random_uuid(), $1, null, $2, NOW());
	`, userId, amount)
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

// transfer Переводит средства со счета пользователя fromId на счет пользователя toId. Перед списанием проверяет и обновляет баланс в таблице accounts у пользователя fromId.
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

// balance Вычисляет баланс у пользователя id и обновляет его в таблице accounts.
func (s *Server) balance(ctx context.Context, userId UserId) (int64, error) {
	tx, err := s.db.BeginTxx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return 0, fmt.Errorf("can't start transaction: %w", err)
	}

	balance, transactionNumSend, transactionNumRecv, err := getBalance(ctx, tx, userId)
	if err != nil {
		err = tx.Rollback()
		return 0, fmt.Errorf("can't get balance: %w", err)
	}

	err = updateBalance(ctx, userId, balance, transactionNumSend, transactionNumRecv, tx)
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

// getBalance Берет из таблицы accounts последниий вычисленный баланс вместе с номерами транзакций зачисления и списания,
// для которых этот баланс был вычислен.
// Далее вычисляет сумму всех операций начисления с номерами больше, чем номер транзакции начисления записанной в таблице accounts,
// Вычисляет сумму всех операций списания с номерами больше, чем номер транзакции списания записанной в таблице ассounts.
// Возвращает максимальный номер транзакции списания, максимальный номер транзакции начисления для этого пользователя и его баланс
func getBalance(ctx context.Context, tx *sqlx.Tx, userId UserId) (balance int64, transactionNumSend int64, transactionNumRecv int64, err error) {
	rowsAccountBalance := tx.QueryRowxContext(
		ctx,
		`SELECT transaction_num_send, transaction_num_recv, sum FROM accounts WHERE user_id = $1`,
		userId)
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
	`, userId, accountBalance.TransactionNumSend, accountBalance.TransactionNumRecv)
	transactionBalance := &RawTransactionBalance{}
	err = rowsTransactionsBalance.StructScan(transactionBalance)
	if err != nil && err != sql.ErrNoRows {
		return 0, 0, 0, fmt.Errorf("can't scan transactionBalance: %w", err)
	}

	return accountBalance.Sum + transactionBalance.Balance, transactionBalance.TransactionNumSend, transactionBalance.TransactionNumRecv, nil
}

// updateBalance Обновляет баланс пользователя в таблице accounts.
// Записывает в таблицу accounts строку со значением последнего номера транзакции, в которой произошло начисление на счет пользователя,
// Последнего номера транзакции, в котором произошло списаниие со счета пользователя
// И баланса, который был посчитан на момент указанных транзакций.
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
