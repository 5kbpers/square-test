package cmd

import (
	"context"

	"github.com/5kbpers/test1/pkg/test"
	"github.com/spf13/cobra"
)

func NewRunCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "run",
		Short: "run test",
		RunE: func(command *cobra.Command, _ []string) error {
			concurrency, err := command.Flags().GetUint64("concurrency")
			if err != nil {
				return err
			}
			operationCount, err := command.Flags().GetUint64("operationcount")
			if err != nil {
				return err
			}
			req3Follower, err := command.Flags().GetBool("req3-follower")
			if err != nil {
				return err
			}
			req4Follower, err := command.Flags().GetBool("req4-follower")
			if err != nil {
				return err
			}
			req1, err := command.Flags().GetUint64("req1")
			if err != nil {
				return err
			}
			req2, err := command.Flags().GetUint64("req2")
			if err != nil {
				return err
			}
			req3, err := command.Flags().GetUint64("req3")
			if err != nil {
				return err
			}
			req4, err := command.Flags().GetUint64("req4")
			if err != nil {
				return err
			}
			dsn, err := command.Flags().GetString("dsn")
			if err != nil {
				return err
			}
			db, err := test.NewDB(dsn, int(concurrency))
			if err != nil {
				return err
			}
			ctx := context.Background()
			errCh := make(chan error, concurrency)
			workers := make([]*test.TestWorker, 0, concurrency)
			for i := uint64(0); i < concurrency; i++ {
				conn, err := db.GetConn(ctx)
				if err != nil {
					return err
				}
				t, err := test.NewTestWorker(ctx, conn, i, req3Follower, req4Follower)
				if err != nil {
					return err
				}
				workers = append(workers, t)
				go func(worker *test.TestWorker) {
					errCh <- worker.Run(operationCount / concurrency, req1, req2, req3, req4)
				}(t)
			}
			defer func() {
				for _, worker := range workers {
					_ = worker.Close()
				}
			}()
			for i := uint64(0); i < concurrency; i++ {
				err = <-errCh
				if err != nil {
					return err
				}
			}
			return nil
		},
	}

	cmd.Flags().Uint64P("concurrency", "c", 200, "the concurrency of client connections")
	cmd.Flags().Uint64P("operationcount", "p", 10000, "the total number of requests")
	cmd.Flags().Uint64("req1", 8, "the weights of request1")
	cmd.Flags().Uint64("req2", 8, "the weights of request2")
	cmd.Flags().Uint64("req3", 81, "the weights of request3")
	cmd.Flags().Uint64("req4", 3, "the weights of request4")
	cmd.Flags().Bool("req3-follower", false, "request3 enable follower read")
	cmd.Flags().Bool("req4-follower", false, "request4 enable follower read")
	cmd.Flags().StringP("dsn", "d", "",
		"the data source name of tidb, eg. username:password@protocol(address)/dbname?param=value")
	_ = cmd.MarkFlagRequired("dsn")
	_ = cmd.MarkFlagRequired("req1")
	_ = cmd.MarkFlagRequired("req2")
	_ = cmd.MarkFlagRequired("req3")
	_ = cmd.MarkFlagRequired("req4")

	return cmd
}

func NewLoadCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "load",
		Short: "load data",
		RunE: func(command *cobra.Command, _ []string) error {
			concurrency, err := command.Flags().GetUint64("concurrency")
			if err != nil {
				return err
			}
			operationCount, err := command.Flags().GetUint64("operationcount")
			if err != nil {
				return err
			}
			dsn, err := command.Flags().GetString("dsn")
			if err != nil {
				return err
			}
			err = test.Init(dsn)
			if err != nil {
				return err
			}
			db, err := test.NewDB(dsn, int(concurrency))
			if err != nil {
				return err
			}
			ctx := context.Background()
			errCh := make(chan error, concurrency)
			workers := make([]*test.TestWorker, 0, concurrency)
			for i := uint64(0); i < concurrency; i++ {
				conn, err := db.GetConn(ctx)
				if err != nil {
					return err
				}
				t, err := test.NewTestWorker(ctx, conn, i, false, false)
				if err != nil {
					return err
				}
				workers = append(workers, t)
				go func(worker *test.TestWorker) {
					errCh <- worker.Load(operationCount / concurrency)
				}(t)
			}
			defer func() {
				for _, worker := range workers {
					_ = worker.Close()
				}
			}()
			for i := uint64(0); i < concurrency; i++ {
				err = <-errCh
				if err != nil {
					return err
				}
			}
			return nil
		},
	}

	cmd.Flags().Uint64P("concurrency", "c", 200, "the concurrency of client connections")
	cmd.Flags().Uint64P("operationcount", "p", 100000, "the total number of requests")
	cmd.Flags().StringP("dsn", "d", "",
		"the data source name of tidb, eg. username:password@protocol(address)/dbname?param=value")
	_ = cmd.MarkFlagRequired("dsn")

	return cmd
}
