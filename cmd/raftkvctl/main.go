package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"raftkv/internal/client"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	seeds := flag.String("seeds",
		"http://localhost:8001,http://localhost:8002,http://localhost:8003",
		"comma-separated member origins")
	timeout := flag.Duration("timeout", 5*time.Second, "overall operation deadline")
	flag.Parse()
	args := flag.Args()
	if len(args) < 2 {
		return fmt.Errorf("usage: raftkvctl [flags] put KEY VALUE | get KEY | delete KEY")
	}
	c, err := client.New(strings.Split(*seeds, ","))
	if err != nil {
		return err
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	switch args[0] {
	case "put":
		if len(args) != 3 {
			return fmt.Errorf("put requires KEY VALUE")
		}
		return c.Put(ctx, args[1], args[2])
	case "get":
		if len(args) != 2 {
			return fmt.Errorf("get requires KEY")
		}
		value, err := c.Get(ctx, args[1])
		if err != nil {
			return err
		}
		fmt.Println(value)
		return nil
	case "delete":
		if len(args) != 2 {
			return fmt.Errorf("delete requires KEY")
		}
		return c.Delete(ctx, args[1])
	default:
		return fmt.Errorf("unknown operation %q", args[0])
	}
}
