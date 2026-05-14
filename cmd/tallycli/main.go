package main

import (
	"net/http"
	"os"

	"github.com/spf13/cobra"
	clihandler "github.com/w-h-a/tally/internal/handler/cli"
)

func main() {
	var (
		url     string
		handler *clihandler.Handler
	)

	rootCmd := &cobra.Command{
		Use:          "tallycli",
		Short:        "CLI client for the Tally distributed commit log",
		SilenceUsage: true,
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			handler = clihandler.New(url, http.DefaultClient)
		},
	}

	rootCmd.PersistentFlags().StringVar(&url, "url", "http://localhost:8080", "tally gateway URL")

	var record string
	produceCmd := &cobra.Command{
		Use:   "produce",
		Short: "Produce a record to the log",
		RunE: func(cmd *cobra.Command, args []string) error {
			return handler.Produce(cmd.Context(), record, cmd.OutOrStdout())
		},
	}
	produceCmd.Flags().StringVar(&record, "record", "", "record value to produce")
	produceCmd.MarkFlagRequired("record")

	var offset uint64
	consumeCmd := &cobra.Command{
		Use:   "consume",
		Short: "Consume a record from the log",
		RunE: func(cmd *cobra.Command, args []string) error {
			return handler.Consume(cmd.Context(), offset, cmd.OutOrStdout())
		},
	}
	consumeCmd.Flags().Uint64Var(&offset, "offset", 0, "offset to consume from")

	serversCmd := &cobra.Command{
		Use:   "servers",
		Short: "List cluster servers",
		RunE: func(cmd *cobra.Command, args []string) error {
			return handler.Servers(cmd.Context(), cmd.OutOrStdout())
		},
	}

	var from uint64
	streamCmd := &cobra.Command{
		Use:   "stream",
		Short: "Stream records from the log",
		RunE: func(cmd *cobra.Command, args []string) error {
			return handler.Stream(cmd.Context(), from, cmd.OutOrStdout())
		},
	}
	streamCmd.Flags().Uint64Var(&from, "from", 0, "offset to stream from")

	rootCmd.AddCommand(produceCmd, consumeCmd, serversCmd, streamCmd)

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
