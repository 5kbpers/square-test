package cmd

import (
	"context"

	"github.com/5kbpers/test1/pkg/test"
	"github.com/spf13/cobra"
)

func NewTestCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "test",
		Short: "run test",
		RunE: func(command *cobra.Command, _ []string) error {
			concurrency, err := command.Flags().GetInt("concurrency")
			if err != nil {
				return err
			}
			operationCount, err := command.Flags().GetInt("operationcount")
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
			db, err := test.NewDB(dsn, concurrency)
			if err != nil {
				return err
			}
			ctx := context.Background()
			errCh := make(chan error, concurrency)
			workers := make([]*test.TestWorker, 0, concurrency)
			for i := 0; i < concurrency; i++ {
				conn, err := db.GetConn(ctx)
				if err != nil {
					return err
				}
				t, err := test.NewSquareTestWorker(ctx, conn, i, dsn)
				if err != nil {
					return err
				}
				workers = append(workers, t)
				go func(worker *test.TestWorker) {
					e := worker.Run(operationCount / concurrency)
					if e != nil {
						errCh <- e
						return
					}
					errCh <- nil
				}(t)
			}
			defer func() {
				for _, worker := range workers {
					_ = worker.Close()
				}
			}()
			for err = range errCh {
				if err != nil {
					return err
				}
			}
			return nil
		},
	}

	cmd.Flags().IntP("concurrency", "c", 200, "the concurrency of client connections")

	cmd.Flags().IntP("operationcount", "p", 10000, "the total number of requests")

	cmd.Flags().StringP("dsn", "d", "",
		"the data source name of tidb, eg. username:password@protocol(address)/dbname?param=value")
	_ = cmd.MarkFlagRequired("dsn")

	return cmd
}
