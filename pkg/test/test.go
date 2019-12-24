package test

import (
	"database/sql"
	"math/rand"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type SquareTestWorker struct {
	workerID       int
	nextCustomerID uint64
	db             *sql.DB
	numGen         *rand.Rand
}

func NewSquareTestWorker(workerID int, dsn string) (*SquareTestWorker, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	return &SquareTestWorker{
		workerID: workerID,
		db:       db,
		numGen:   rand.New(rand.NewSource(time.Now().UnixNano())),
	}, nil
}

func (t *SquareTestWorker) Run(operationCount int) error {
	t.nextCustomerID = uint64(t.workerID * operationCount * 50)
	err := withRetry(t.request1, 10)
	if err != nil {
		return err
	}
	for i := 0; i < operationCount; i++ {
		req := t.numGen.Uint64() % 4
		switch req {
		case 0:
			err = withRetry(t.request1, 10)
		case 1:
			err = withRetry(t.request2, 10)
		case 2:
			err = withRetry(t.request3, 10)
		case 3:
			err = withRetry(t.request4, 10)
		}
		if err != nil {
			log.Error("Run request failed", zap.Uint64("requestID", req+1), zap.Error(err))
			return err
		}
	}
	return nil
}

func (t *SquareTestWorker) Close() error {
	return t.db.Close()
}

func (t *SquareTestWorker) request1() error {
	log.Info("Run request1", zap.Int("workerID", t.workerID))
	for i := 0; i < 50; i++ {
		id := t.nextCustomerID
		t.nextCustomerID++
		name := "customer" + string(id)
		_, err := t.db.Exec(InsertCustomerSQL, id, name)
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *SquareTestWorker) request2() error {
	log.Info("Run request2", zap.Int("workerID", t.workerID))
	for i := 0; i < 10; i++ {
		var customerID, counterpartyID uint64
		for customerID == counterpartyID {
			customerID = t.getRandomCustomerID()
			counterpartyID = t.getRandomCustomerID()
		}
		tx, err := t.db.Begin()
		if err != nil {
			return err
		}
		_, err = tx.Exec(InsertMovementSQL, "SENDER", customerID, counterpartyID, t.numGen.Int63(), t.numGen.Int63())
		if err != nil {
			return tx.Rollback()
		}
		_, err = tx.Exec(InsertMovementSQL, "RECIPIENT", counterpartyID, customerID, t.numGen.Int63(), t.numGen.Int63())
		if err != nil {
			return tx.Rollback()
		}
		err = tx.Commit()
		if err != nil {
			return tx.Rollback()
		}
	}
	return nil
}

func (t *SquareTestWorker) request3() error {
	log.Info("Run request3", zap.Int("workerID", t.workerID))
	for i := 0; i < 50; i++ {
		var customerID, counterpartyID uint64
		for customerID == counterpartyID {
			customerID = t.getRandomCustomerID()
			counterpartyID = t.getRandomCustomerID()
		}
		_, err := t.db.Query(QueryCustomerMovementsSQL, customerID)
		if err != nil {
			return err
		}
		_, err = t.db.Query(QueryCustomerMovementsSQL, counterpartyID)
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *SquareTestWorker) request4() error {
	log.Info("Run request4", zap.Int("workerID", t.workerID))
	_, err := t.db.Exec("set @@tidb_replica_read='follower'")
	if err != nil {
		return err
	}
	_, err = t.db.Query(QueryAllMovementsSQL)
	return err
}

func (t *SquareTestWorker) getRandomCustomerID() uint64 {
	return t.numGen.Uint64() % t.nextCustomerID
}

type retryableFunc func() error

func withRetry(
	retryableFunc retryableFunc,
	attempts uint,
) error {
	var lastErr error
	for i := uint(0); i < attempts; i++ {
		err := retryableFunc()
		if err != nil {
			lastErr = err
			// If this is the last attempt, do not wait
			if i == attempts-1 {
				break
			}
		} else {
			return nil
		}
	}
	return lastErr
}

func Init(dsn string) error {
	log.Info("Init test...")
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return err
	}
	if _, err := db.Exec(CreateCustomersSQL); err != nil {
		return err
	}
	if _, err := db.Exec(CreateMovementsSQL); err != nil {
		return err
	}
	return nil
}
