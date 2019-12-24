package test

import (
	"context"
	"database/sql"
	"math/rand"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type TestWorker struct {
	workerID       int
	nextCustomerID uint64
	conn           *Conn
	numGen         *rand.Rand
	ctx            context.Context
	opCount        int
	followerRead   bool
}

func NewTestWorker(ctx context.Context, conn *Conn, workerID int, dsn string, followerRead bool) (*TestWorker, error) {
	return &TestWorker{
		ctx:      ctx,
		workerID: workerID,
		conn:     conn,
		numGen:   rand.New(rand.NewSource(time.Now().UnixNano())),
		followerRead: followerRead,
	}, nil
}

func (t *TestWorker) Run(operationCount int) error {
	t.nextCustomerID = uint64(t.workerID * operationCount * 50)
	err := withRetry(t.request1, 10)
	if err != nil {
		return err
	}
	for i := 0; i < operationCount; i++ {
		t.opCount++
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
			return errors.Trace(err)
		}
	}
	return nil
}

func (t *TestWorker) Close() error {
	return t.conn.Close()
}

func (t *TestWorker) request1() error {
	log.Info("Run request1", zap.Int("workerID", t.workerID), zap.Int("count", t.opCount))
	for i := 0; i < 50; i++ {
		id := t.nextCustomerID
		t.nextCustomerID++
		name := "customer" + string(id)
		err := t.conn.execQuery(t.ctx, InsertCustomerSQL, id, name)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (t *TestWorker) request2() error {
	log.Info("Run request2", zap.Int("workerID", t.workerID), zap.Int("count", t.opCount))
	for i := 0; i < 10; i++ {
		var customerID, counterpartyID uint64
		for customerID == counterpartyID {
			customerID = t.getRandomCustomerID()
			counterpartyID = t.getRandomCustomerID()
		}
		tx, err := t.conn.beginTx(t.ctx)
		if err != nil {
			return errors.Trace(err)
		}
		_, err = tx.Exec(InsertMovementSQL, "SENDER", customerID, counterpartyID, t.numGen.Int63(), t.numGen.Int63())
		if err != nil {
			return errors.Trace(tx.Rollback())
		}
		_, err = tx.Exec(InsertMovementSQL, "RECIPIENT", counterpartyID, customerID, t.numGen.Int63(), t.numGen.Int63())
		if err != nil {
			return errors.Trace(tx.Rollback())
		}
		err = tx.Commit()
		if err != nil {
			return errors.Trace(tx.Rollback())
		}
	}
	return nil
}

func (t *TestWorker) request3() error {
	log.Info("Run request3", zap.Int("workerID", t.workerID), zap.Int("count", t.opCount))
	for i := 0; i < 50; i++ {
		var customerID, counterpartyID uint64
		for customerID == counterpartyID {
			customerID = t.getRandomCustomerID()
			counterpartyID = t.getRandomCustomerID()
		}
		err := t.conn.execQuery(t.ctx, QueryCustomerMovementsSQL, customerID)
		if err != nil {
			return errors.Trace(err)
		}
		err = t.conn.execQuery(t.ctx, QueryCustomerMovementsSQL, counterpartyID)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (t *TestWorker) request4() error {
	log.Info("Run request4", zap.Int("workerID", t.workerID), zap.Int("count", t.opCount))
	var err error
	if t.followerRead {
		err = t.conn.execQuery(t.ctx, "set @@tidb_replica_read='follower'")
		if err != nil {
			return errors.Trace(err)
		}
	}
	err = t.conn.execQuery(t.ctx, QueryAllMovementsSQL)
	if err != nil {
		return errors.Trace(err)
	}
	if t.followerRead {
		err = t.conn.execQuery(t.ctx, "set @@tidb_replica_read='leader'")
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (t *TestWorker) getRandomCustomerID() uint64 {
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
