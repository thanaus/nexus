package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	parquet "github.com/parquet-go/parquet-go"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
)

const (
	AppName    = "nexus"
	AppNameCap = "Nexus"
)

const (
	TypeUnknown uint8 = 0
	TypePipe    uint8 = 1
	TypeCharDev uint8 = 2
	TypeDir     uint8 = 4
	TypeDevice  uint8 = 6
	TypeFile    uint8 = 8
	TypeSymlink uint8 = 10
	TypeSocket  uint8 = 12
)

type FileRecord struct {
	Path    string `parquet:"path"`
	Inode   uint64 `parquet:"inode"`
	Type    uint8  `parquet:"type"`
	Size    int64  `parquet:"size"`
	MTime   int64  `parquet:"mtime"`
	CTime   int64  `parquet:"ctime"`
}

type FileEntry struct {
	path     string
	fileType uint8
}

var rootCmd = &cobra.Command{
	Use:   AppName,
	Short: AppNameCap + " — filesystem metadata scanner",
	CompletionOptions: cobra.CompletionOptions{
		DisableDefaultCmd: true,
	},
}

func Execute() error {
	return rootCmd.Execute()
}

func init() {
	rootCmd.AddCommand(newLsCmd())
}

func newLsCmd() *cobra.Command {
	var parquetOut, natsURL, natsStream, natsSubject string
	var workers int
	var all bool

	cmd := &cobra.Command{
		Use:   "ls <directory>",
		Short: "List objects in a directory and collect metadata",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runLs(args[0], parquetOut, natsURL, natsStream, natsSubject, workers, all)
		},
	}

	cmd.Flags().StringVarP(&parquetOut, "parquet", "p", "", "output parquet file (optional)")
	cmd.Flags().StringVarP(&natsURL, "nats", "n", "", "NATS server URL (ex: nats://localhost:4222)")
	cmd.Flags().StringVar(&natsStream, "nats-stream", AppName, "JetStream stream name")
	cmd.Flags().StringVar(&natsSubject, "nats-subject", AppName+".ls", "JetStream subject")
	cmd.Flags().IntVarP(&workers, "workers", "w", runtime.NumCPU()*2, "number of parallel goroutines")
	cmd.Flags().BoolVarP(&all, "all", "a", false, "retrieve size and additional metadata (size, mode, mtime, ctime, inode)")
	cmd.MarkFlagsMutuallyExclusive("parquet", "nats")

	return cmd
}

func runLs(dir, parquetOut, natsURL, natsStream, natsSubject string, workers int, all bool) error {
	absDir, err := filepath.Abs(dir)
	if err != nil {
		return fmt.Errorf("invalid path: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	signalCtx := ctx

	start := time.Now()

	var objectCount, totalSize atomic.Int64

	jobs := make(chan FileEntry, workers*4)
	g, ctx := errgroup.WithContext(ctx)

	var (
		results  chan FileRecord
		workerWg sync.WaitGroup
	)
	if parquetOut != "" {
		results = make(chan FileRecord, workers*4)

		g.Go(func() error {
			f, err := os.Create(parquetOut)
			if err != nil {
				return fmt.Errorf("create parquet file: %w", err)
			}
			defer f.Close()

			writer := parquet.NewGenericWriter[FileRecord](f)
			batch := make([]FileRecord, 0, 10_000)

			flush := func() error {
				if len(batch) == 0 {
					return nil
				}
				if _, err := writer.Write(batch); err != nil {
					return fmt.Errorf("write parquet batch: %w", err)
				}
				batch = batch[:0]
				if err := writer.Flush(); err != nil {
					return fmt.Errorf("flush parquet row group: %w", err)
				}
				return nil
			}

			for rec := range results {
				batch = append(batch, rec)
				if len(batch) >= 10_000 {
					if err := flush(); err != nil {
						return err
					}
				}
			}
			if err := flush(); err != nil {
				return err
			}
			if err := writer.Close(); err != nil {
				return fmt.Errorf("close parquet writer: %w", err)
			}
			return nil
		})

		g.Go(func() error {
			workerWg.Wait()
			close(results)
			return nil
		})
	} else if natsURL != "" {
		results = make(chan FileRecord, workers*4)

		g.Go(func() error {
			nc, err := nats.Connect(natsURL)
			if err != nil {
				return fmt.Errorf("connect to NATS: %w", err)
			}
			defer nc.Drain()

			js, err := nc.JetStream()
			if err != nil {
				return fmt.Errorf("JetStream context: %w", err)
			}

			if _, err := js.StreamInfo(natsStream); err == nats.ErrStreamNotFound {
				if _, err := js.AddStream(&nats.StreamConfig{
					Name:     natsStream,
					Subjects: []string{natsSubject},
				}); err != nil {
					return fmt.Errorf("create NATS stream: %w", err)
				}
			} else if err != nil {
				return fmt.Errorf("check NATS stream: %w", err)
			}

			for rec := range results {
				data, err := json.Marshal(rec)
				if err != nil {
					return fmt.Errorf("marshal record: %w", err)
				}
				if _, err := js.Publish(natsSubject, data); err != nil {
					return fmt.Errorf("publish to NATS: %w", err)
				}
			}
			return nil
		})

		g.Go(func() error {
			workerWg.Wait()
			close(results)
			return nil
		})
	}

	scanDone := make(chan struct{})
	go func() {
		printProgress := func(t time.Time, rate, cpuPct float64) {
			line := fmt.Sprintf("\r\033[K[%s] %s objects",
				compactDuration(t.Sub(start)),
				humanSI(objectCount.Load()),
			)
			if all {
				line += fmt.Sprintf(" • %s", formatSize(totalSize.Load()))
			}
			line += fmt.Sprintf(" • %s obj/s • cpu %.0f%% • rss %s",
				humanSI(int64(rate)),
				cpuPct,
				humanSI(readRSS()),
			)
			fmt.Fprint(os.Stderr, line)
		}
		printProgress(start, 0, 0)
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		var prevCount, prevUser, prevSys int64
		prevTime := start
		for {
			select {
			case t := <-ticker.C:
				count := objectCount.Load()
				rate  := float64(count-prevCount) / t.Sub(prevTime).Seconds()

				var ru syscall.Rusage
				syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
				curUser   := ru.Utime.Sec*1_000_000 + int64(ru.Utime.Usec)
				curSys    := ru.Stime.Sec*1_000_000 + int64(ru.Stime.Usec)
				cpuPct    := float64((curUser-prevUser)+(curSys-prevSys)) / float64(t.Sub(prevTime).Microseconds()) * 100

				prevCount = count
				prevTime  = t
				prevUser  = curUser
				prevSys   = curSys
				printProgress(t, rate, cpuPct)
			case <-scanDone:
				return
			}
		}
	}()

	for range workers {
		workerWg.Add(1)
		g.Go(func() error {
			defer workerWg.Done()
			for {
				select {
				case fe, ok := <-jobs:
					if !ok {
						return nil
					}
					rec := FileRecord{
						Path: fe.path,
						Type: fe.fileType,
					}
					if all {
						info, err := os.Lstat(fe.path)
						if err != nil {
							// file removed between directory read and stat
							continue
						}
						st := info.Sys().(*syscall.Stat_t)
						rec.Inode   = st.Ino
						rec.Size    = info.Size()
						rec.MTime   = info.ModTime().Unix()
						rec.CTime   = st.Ctim.Sec
						totalSize.Add(info.Size())
					}
					objectCount.Add(1)
					if results != nil {
						select {
						case results <- rec:
						case <-ctx.Done():
							return ctx.Err()
						}
					}
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		})
	}

	g.Go(func() error {
		defer close(jobs)

		f, err := os.Open(absDir)
		if err != nil {
			return fmt.Errorf("open directory: %w", err)
		}
		defer f.Close()

		for {
			entries, err := f.ReadDir(1024)
			for _, entry := range entries {
				fe := FileEntry{
					path:     filepath.Join(absDir, entry.Name()),
					fileType: modeToType(entry.Type()),
				}
				select {
				case jobs <- fe:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return fmt.Errorf("read directory: %w", err)
			}
		}
	})

	if err := g.Wait(); err != nil && err != context.Canceled {
		close(scanDone)
		return err
	}
	close(scanDone)

	fmt.Fprintf(os.Stderr, "\r\033[K")
	if signalCtx.Err() != nil {
		fmt.Printf("⚠ Scan interrupted\n\n")
	} else {
		fmt.Printf("✔ Scan completed\n\n")
	}
	fmt.Printf("  Objects scanned : %s\n", humanSI(objectCount.Load()))
	if all {
		fmt.Printf("  Total size    : %s\n", formatSize(totalSize.Load()))
	}
	fmt.Printf("  Duration      : %s\n", time.Since(start).Round(time.Millisecond))

	return nil
}

func modeToType(m fs.FileMode) uint8 {
	t := m.Type()
	switch {
	case t == 0:
		return TypeFile
	case t&fs.ModeDir != 0:
		return TypeDir
	case t&fs.ModeSymlink != 0:
		return TypeSymlink
	case t&fs.ModeDevice != 0 && t&fs.ModeCharDevice != 0:
		return TypeCharDev
	case t&fs.ModeDevice != 0:
		return TypeDevice
	case t&fs.ModeNamedPipe != 0:
		return TypePipe
	case t&fs.ModeSocket != 0:
		return TypeSocket
	default:
		return TypeUnknown
	}
}

func readRSS() int64 {
	data, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0
	}
	var dummy, rss int64
	fmt.Sscanf(string(data), "%d %d", &dummy, &rss)
	return rss * int64(os.Getpagesize())
}

func compactDuration(d time.Duration) string {
	d = d.Round(time.Second)
	h := d / time.Hour
	d -= h * time.Hour
	m := d / time.Minute
	d -= m * time.Minute
	s := d / time.Second
	if h > 0 {
		return fmt.Sprintf("%dh%02dm%02ds", h, m, s)
	}
	if m > 0 {
		return fmt.Sprintf("%dm%02ds", m, s)
	}
	return fmt.Sprintf("%ds", s)
}

func humanSI(n int64) string {
	switch {
	case n >= 1_000_000_000:
		return fmt.Sprintf("%.1fG", float64(n)/1e9)
	case n >= 1_000_000:
		return fmt.Sprintf("%.1fM", float64(n)/1e6)
	case n >= 1_000:
		return fmt.Sprintf("%.1fK", float64(n)/1e3)
	default:
		return fmt.Sprintf("%d", n)
	}
}

func formatSize(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(bytes)/float64(div), "KMGTPE"[exp])
}
