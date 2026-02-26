package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/client"
	"github.com/pingcap/go-ycsb/pkg/measurement"
	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/util"
	_ "github.com/pingcap/go-ycsb/pkg/workload"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
	"github.com/spf13/cobra"

	_ "github.com/Mulily0513/C2KV/eval/ycsb-runner/db/etcd"
	"github.com/Mulily0513/C2KV/eval/ycsb-runner/db/retry"
)

var (
	propertyFiles  []string
	propertyValues []string
	tableName      string
	threadsArg     int
	targetArg      int
	reportInterval int

	globalContext context.Context
	globalCancel  context.CancelFunc

	globalDB       ycsb.DB
	globalWorkload ycsb.Workload
	globalProps    *properties.Properties
)

func initialGlobal(dbName string, onProperties func()) {
	globalProps = properties.NewProperties()
	if len(propertyFiles) > 0 {
		globalProps = properties.MustLoadFiles(propertyFiles, properties.UTF8, false)
	}

	for _, p := range propertyValues {
		seps := strings.SplitN(p, "=", 2)
		if len(seps) != 2 {
			log.Fatalf("bad property: `%s`, expected format `name=value`", p)
		}
		globalProps.Set(seps[0], seps[1])
	}

	if onProperties != nil {
		onProperties()
	}

	measurement.InitMeasure(globalProps)

	if len(tableName) == 0 {
		tableName = globalProps.GetString(prop.TableName, prop.TableNameDefault)
	}
	if _, _, err := globalProps.Set(prop.TableName, tableName); err != nil {
		panic(err)
	}

	workloadName := globalProps.GetString(prop.Workload, "core")
	workloadCreator := ycsb.GetWorkloadCreator(workloadName)
	if workloadCreator == nil {
		util.Fatalf("workload %s is not registered", workloadName)
	}

	var err error
	if globalWorkload, err = workloadCreator.Create(globalProps); err != nil {
		util.Fatalf("create workload %s failed %v", workloadName, err)
	}

	dbCreator := ycsb.GetDBCreator(dbName)
	if dbCreator == nil {
		util.Fatalf("%s is not registered", dbName)
	}
	if globalDB, err = dbCreator.Create(globalProps); err != nil {
		util.Fatalf("create db %s failed %v", dbName, err)
	}
	retryAttempts := globalProps.GetInt("runner.retry_attempts", 3)
	retryBackoff := time.Duration(globalProps.GetInt("runner.retry_backoff_ms", 10)) * time.Millisecond
	globalDB = retry.Wrap(globalDB, retryAttempts, retryBackoff)
	globalDB = client.DbWrapper{DB: globalDB}
}

func runClientCommandFunc(cmd *cobra.Command, args []string, doTransactions bool, command string) {
	dbName := args[0]
	initialGlobal(dbName, func() {
		doTransFlag := "true"
		if !doTransactions {
			doTransFlag = "false"
		}
		globalProps.Set(prop.DoTransactions, doTransFlag)
		globalProps.Set(prop.Command, command)

		if cmd.Flags().Changed("threads") {
			globalProps.Set(prop.ThreadCount, strconv.Itoa(threadsArg))
		}
		if cmd.Flags().Changed("target") {
			globalProps.Set(prop.Target, strconv.Itoa(targetArg))
		}
		if cmd.Flags().Changed("interval") {
			globalProps.Set(prop.LogInterval, strconv.Itoa(reportInterval))
		}
	})

	fmt.Println("***************** properties *****************")
	for key, value := range globalProps.Map() {
		fmt.Printf("\"%s\"=\"%s\"\n", key, value)
	}
	fmt.Println("**********************************************")

	c := client.NewClient(globalProps, globalWorkload, globalDB)
	start := time.Now()
	c.Run(globalContext)
	fmt.Println("**********************************************")
	fmt.Printf("Run finished, takes %s\n", time.Since(start))
	measurement.Output()
}

func newLoadCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "load db",
		Short: "YCSB load benchmark",
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			runClientCommandFunc(cmd, args, false, "load")
		},
	}
	initClientCommand(cmd)
	return cmd
}

func newRunCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "run db",
		Short: "YCSB run benchmark",
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			runClientCommandFunc(cmd, args, true, "run")
		},
	}
	initClientCommand(cmd)
	return cmd
}

func initClientCommand(cmd *cobra.Command) {
	cmd.Flags().StringSliceVarP(&propertyFiles, "property_file", "P", nil, "Specify property file")
	cmd.Flags().StringArrayVarP(&propertyValues, "prop", "p", nil, "Specify property value with name=value")
	cmd.Flags().StringVar(&tableName, "table", "", "Use the table name instead of default \"usertable\"")
	cmd.Flags().IntVar(&threadsArg, "threads", 1, "Execute using n threads")
	cmd.Flags().IntVar(&targetArg, "target", 0, "Attempt n ops/sec")
	cmd.Flags().IntVar(&reportInterval, "interval", 10, "Measurement output interval in seconds")
}

func main() {
	globalContext, globalCancel = context.WithCancel(context.Background())
	defer globalCancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	closeDone := make(chan struct{}, 1)
	go func() {
		sig := <-sigCh
		fmt.Printf("\nGot signal [%v] to exit.\n", sig)
		globalCancel()
		select {
		case <-sigCh:
			fmt.Println("\nGot second signal, force exit")
			os.Exit(1)
		case <-time.After(10 * time.Second):
			fmt.Println("\nWaited 10s, force exit")
			os.Exit(1)
		case <-closeDone:
			return
		}
	}()

	rootCmd := &cobra.Command{Use: "go-ycsb-etcd", Short: "Go YCSB runner for etcd"}
	rootCmd.AddCommand(newLoadCommand(), newRunCommand())

	cobra.EnablePrefixMatching = true
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(rootCmd.UsageString())
		os.Exit(1)
	}

	if globalDB != nil {
		_ = globalDB.Close()
	}
	if globalWorkload != nil {
		globalWorkload.Close()
	}
	closeDone <- struct{}{}
}
