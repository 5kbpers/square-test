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
	workerID     uint64
	customerIDs  []int64
	conn         *Conn
	numGen       *rand.Rand
	ctx          context.Context
	opCount      int
	req3Follower bool
	req4Follower bool
}

type TestResult struct {
	Err error
	Req [4]RequestResult
}

type RequestResult struct {
	Count int
	Take  time.Duration
}

func NewTestWorker(ctx context.Context, conn *Conn, workerID uint64, req3Follower bool, req4Follower bool) (*TestWorker, error) {
	return &TestWorker{
		ctx:          ctx,
		workerID:     workerID,
		conn:         conn,
		numGen:       rand.New(rand.NewSource(time.Now().UnixNano())),
		req3Follower: req3Follower,
		req4Follower: req4Follower,
	}, nil
}

func (t *TestWorker) Run(operationCount, req1, req2, req3, req4 uint64) *TestResult {
	res := new(TestResult)
	err := withRetry(t.request1, 10)
	if err != nil {
		res.Err = err
		return res
	}
	for i := uint64(0); i < operationCount; i++ {
		t.opCount++
		startTime := time.Now()
		req := t.numGen.Uint64() % (req1 + req2 + req3 + req4)
		switch {
		case req < req1:
			err = withRetry(t.request1, 10)
			res.Req[0].Count++
			res.Req[0].Take += time.Since(startTime)
		case req >= req1 && req < req1+req2:
			err = withRetry(t.request2, 10)
			res.Req[1].Count++
			res.Req[1].Take += time.Since(startTime)
		case req >= req1+req2 && req < req1+req2+req3:
			err = withRetry(t.request4, 10)
			res.Req[2].Count++
			res.Req[2].Take += time.Since(startTime)
		case req >= req1+req2+req3:
			err = withRetry(t.request3, 10)
			res.Req[3].Count++
			res.Req[3].Take += time.Since(startTime)
		}
		if err != nil {
			log.Error("Run request failed", zap.Uint64("requestID", req+1), zap.Error(err), zap.Uint64("workerID", t.workerID))
			res.Err = err
			return res
		}
	}
	return res
}

func (t *TestWorker) Load(operationCount uint64) error {
	err := t.conn.disableAutocommit(t.ctx)
	if err != nil {
		return err
	}
	for i := uint64(0); i < operationCount; i++ {
		err = withRetry(func() error {
			name := "customer"
			rs, err := t.conn.execQueryNoCommit(t.ctx, InsertCustomerSQL, name)
			if err != nil {
				return errors.Trace(err)
			}
			lastInsertID, err := rs.LastInsertId()
			if err != nil {
				return errors.Trace(err)
			}
			t.customerIDs = append(t.customerIDs, lastInsertID)
			return nil
		}, 10)
		log.Info("Create customer", zap.Uint64("workerID", t.workerID), zap.Uint64("count", i+1))
		if err != nil {
			log.Error("Create customers failed", zap.Error(err))
			return errors.Trace(err)
		}
	}
	err = t.conn.commit(t.ctx)
	if err != nil {
		log.Error("Create customers failed", zap.Error(err))
		return errors.Trace(err)
	}

	for i := uint64(0); i < operationCount; i++ {
		for j := 0; j < 1000; j++ {
			err := withRetry(func() error {
				var customerID, counterpartyID int64
				for customerID == counterpartyID {
					customerID = t.getRandomCustomerID()
					counterpartyID = t.getRandomCustomerID()
				}
				_, err := t.conn.execQueryNoCommit(t.ctx, InsertMovementSQL, "SENDER", customerID, counterpartyID, t.numGen.Int63(), t.numGen.Int63())
				if err != nil {
					return err
				}
				_, err = t.conn.execQueryNoCommit(t.ctx, InsertMovementSQL, "RECIPIENT", counterpartyID, customerID, t.numGen.Int63(), t.numGen.Int63())
				return err
			}, 10)
			if err != nil {
				log.Error("Create movements failed", zap.Error(err))
				return errors.Trace(err)
			}
		}
		log.Info("Create movements", zap.Uint64("workerID", t.workerID), zap.Uint64("count", (i+1)*1000))
		err = t.conn.commit(t.ctx)
		if err != nil {
			log.Error("Create movements failed", zap.Error(err))
			return errors.Trace(err)
		}
	}
	return nil
}

func (t *TestWorker) Close() error {
	return t.conn.Close()
}

func (t *TestWorker) request1() error {
	log.Info("Run request1", zap.Uint64("workerID", t.workerID), zap.Int("count", t.opCount))
	for i := 0; i < 50; i++ {
		name := "customer"
		rs, err := t.conn.execQuery(t.ctx, InsertCustomerSQL, name)
		if err != nil {
			return errors.Trace(err)
		}
		lastInsertID, err := rs.LastInsertId()
		if err != nil {
			return errors.Trace(err)
		}
		t.customerIDs = append(t.customerIDs, lastInsertID)
	}
	return nil
}

func (t *TestWorker) request2() error {
	log.Info("Run request2", zap.Uint64("workerID", t.workerID), zap.Int("count", t.opCount))
	for i := 0; i < 10; i++ {
		var customerID, counterpartyID int64
		for customerID == counterpartyID {
			customerID = t.getRandomCustomerID()
			counterpartyID = t.getRandomCustomerID()
		}
		_, err := t.conn.execQuery(t.ctx, InsertMovementSQL, "SENDER", customerID, counterpartyID, t.numGen.Int63(), t.numGen.Int63())
		if err != nil {
			return err
		}
		_, err = t.conn.execQuery(t.ctx, InsertMovementSQL, "RECIPIENT", counterpartyID, customerID, t.numGen.Int63(), t.numGen.Int63())
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *TestWorker) request3() error {
	log.Info("Run request3", zap.Uint64("workerID", t.workerID), zap.Int("count", t.opCount))
	var err error
	if t.req3Follower {
		_, err = t.conn.execQuery(t.ctx, "set @@tidb_replica_read='follower'")
		if err != nil {
			return errors.Trace(err)
		}
	}
	for i := 0; i < 50; i++ {
		var customerID, counterpartyID int64
		for customerID == counterpartyID {
			customerID = t.getRandomCustomerID()
			counterpartyID = t.getRandomCustomerID()
		}
		_, err = t.conn.execQuery(t.ctx, QueryCustomerMovementsSQL, customerID)
		if err != nil {
			return errors.Trace(err)
		}
		_, err = t.conn.execQuery(t.ctx, QueryCustomerMovementsSQL, counterpartyID)
		if err != nil {
			return errors.Trace(err)
		}
	}
	if t.req3Follower {
		_, err = t.conn.execQuery(t.ctx, "set @@tidb_replica_read='leader'")
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (t *TestWorker) request4() error {
	log.Info("Run request4", zap.Uint64("workerID", t.workerID), zap.Int("count", t.opCount))
	var err error
	if t.req4Follower {
		_, err = t.conn.execQuery(t.ctx, "set @@tidb_replica_read='follower'")
		if err != nil {
			return errors.Trace(err)
		}
	}
	_, err = t.conn.execQuery(t.ctx, QueryAllMovementsSQL)
	if err != nil {
		return errors.Trace(err)
	}
	if t.req4Follower {
		_, err = t.conn.execQuery(t.ctx, "set @@tidb_replica_read='leader'")
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (t *TestWorker) getRandomCustomerID() int64 {
	return t.customerIDs[int(t.numGen.Uint32())%len(t.customerIDs)]
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
