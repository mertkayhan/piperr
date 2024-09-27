package cmd

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/mertkayhan/piperr/pkg/catalog"
	"github.com/mertkayhan/piperr/pkg/config"
	"github.com/mertkayhan/piperr/pkg/destination"
	"github.com/mertkayhan/piperr/pkg/source"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func checkArgs() error {
	if !viper.IsSet("config") {
		return fmt.Errorf("config path is not set")
	}
	if !viper.IsSet("state") {
		return fmt.Errorf("state path is not set")
	}
	if !viper.IsSet("catalog") {
		return fmt.Errorf("catalog path is not set")
	}
	return nil
}

func piperrRun(cmd *cobra.Command, args []string) {
	if err := checkArgs(); err != nil {
		log.Fatal(err)
	}
	streamCatalog, err := catalog.BuildCatalog()
	if err != nil {
		log.Fatal(err)
	}
	config, err := config.BuildConfig()
	if err != nil {
		log.Fatal(err)
	}
	pipePath := fmt.Sprintf("/tmp/pipe-%d", time.Now().UnixMilli())
	if err := syscall.Mkfifo(pipePath, 0666); err != nil {
		log.Fatal(err)
	}
	defer os.Remove(pipePath)
	log.Println("created pipe", pipePath)
	wg := &sync.WaitGroup{}
	wg.Add(2)
	errCh := make(chan error)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go source.Run(ctx, wg, errCh, pipePath, config)
	go destination.Run(ctx, wg, errCh, pipePath, config, streamCatalog)
	go func() {
		wg.Wait()
		close(errCh)
	}()
	if err, ok := <-errCh; ok {
		log.Fatal(err)
	}
}
