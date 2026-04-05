package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"nexus/internal/app"
	"nexus/internal/format"
	"nexus/internal/osutil"

	"github.com/nats-io/nats.go"
	parquet "github.com/parquet-go/parquet-go"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
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

	parquetBatchRows = 10_000
	readDirChunk     = 1024
	progressInterval = 30 * time.Second

	kvKeyLsDone = "ls_done"
)

// FileRecord is one filesystem object with optional extended metadata (Parquet / NATS JSON).
type FileRecord struct {
	Path  string `parquet:"path" json:"path"`
	Inode uint64 `parquet:"inode" json:"inode"`
	Type  uint8  `parquet:"type" json:"type"`
	Size  int64  `parquet:"size" json:"size"`
	MTime int64  `parquet:"mtime" json:"mtime"`
	CTime int64  `parquet:"ctime" json:"ctime"`
}

type FileEntry struct {
	path     string
	fileType uint8
}

func newLsCmd() *cobra.Command {
	var parquetOut, natsURL, token string
	var workers int
	var all bool

	cmd := &cobra.Command{
		Use:     "ls [directory]",
		GroupID: groupCore,
		Short:   "List directory contents",
		Long: "With --nats and --token (after nexus sync), [directory] is optional: the path from the job is used; " +
			"you can pass [directory] to override it. Otherwise [directory] is required.",
		Args: func(cmd *cobra.Command, args []string) error {
			// MarkFlagsRequiredTogether("nats", "token") already guarantees that
			// both or neither are set before Args is called — no need to re-check.
			jobMode := strings.TrimSpace(natsURL) != ""
			if jobMode {
				if len(args) > 1 {
					return fmt.Errorf("accepts at most one directory argument in job mode; received %d", len(args))
				}
				return nil
			}
			if err := cobra.ExactArgs(1)(cmd, args); err != nil {
				return fmt.Errorf("requires <directory> (unless using --nats and --token)")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			var dir string
			if len(args) > 0 {
				dir = args[0]
			}
			return runLs(cmd.Context(), dir, parquetOut, natsURL, token, workers, all, cmd.OutOrStdout(), cmd.ErrOrStderr())
		},
		Example: fmt.Sprintf(`  # Scan directory (path and type only)
  %s ls /var/data

  # Extended metadata to a Parquet file
  %s ls /var/data --parquet /tmp/meta.parquet --all

  # Job mode: scan path from sync config (no directory argument)
  %s ls --nats nats://localhost:4222 --token <job-uuid> --all

  # Job mode: override directory
  %s ls /other --nats nats://localhost:4222 --token <job-uuid> --all`,
			app.Name, app.Name, app.Name, app.Name),
	}

	cmd.Flags().StringVarP(&parquetOut, "parquet", "p", "", "output parquet file (optional)")
	cmd.Flags().StringVarP(&natsURL, "nats", "n", "", "NATS broker URL (required with --token)")
	cmd.Flags().StringVarP(&token, "token", "t", "", "job id printed by nexus sync (stdout)")
	cmd.Flags().IntVarP(&workers, "workers", "w", runtime.NumCPU()*2, "number of parallel goroutines")
	cmd.Flags().BoolVarP(&all, "all", "a", false, "retrieve size and additional metadata (size, mode, mtime, ctime, inode)")

	cmd.MarkFlagsRequiredTogether("nats", "token")
	cmd.MarkFlagsMutuallyExclusive("parquet", "nats")
	cmd.MarkFlagsMutuallyExclusive("parquet", "token")

	return cmd
}

// runLs receives ctx from cmd.Context() — the signal-aware context propagated
// by ExecuteContext — and writes human output to out/errOut so callers (tests
// included) can inject any io.Writer instead of relying on os.Stdout/os.Stderr.
func runLs(ctx context.Context, dirArg, parquetOut, natsURL, token string, workers int, all bool, out, errOut io.Writer) error {
	token = strings.TrimSpace(token)
	natsURL = strings.TrimSpace(natsURL)
	jobNatsMode := token != ""

	var (
		nc          *nats.Conn
		kv          nats.KeyValue
		js          nats.JetStreamContext
		workSubject string
		scanDir     string
		err         error
	)

	if jobNatsMode {
		nc, err = nats.Connect(natsURL,
			nats.Name(app.Name+"-ls"),
			nats.RetryOnFailedConnect(true),
			nats.MaxReconnects(5),
			nats.ReconnectWait(time.Second),
		)
		if err != nil {
			return fmt.Errorf("broker %q: nats connect: %w", natsURL, err)
		}
		defer nc.Drain()

		js, err = nc.JetStream()
		if err != nil {
			return fmt.Errorf("broker %q: jetstream context: %w", natsURL, err)
		}

		kv, err = js.KeyValue(token)
		if err != nil {
			return fmt.Errorf("broker %q: cannot open job %q (run nexus sync first?): %w", natsURL, token, err)
		}

		entry, err := kv.Get(kvKeyConfig)
		if err != nil {
			return fmt.Errorf("broker %q: cannot read job settings for %q: %w", natsURL, token, err)
		}

		var cfg SyncConfig
		if err := json.Unmarshal(entry.Value(), &cfg); err != nil {
			return fmt.Errorf("invalid job settings: %w", err)
		}
		workSubject = cfg.WorkSubject
		if workSubject == "" {
			return fmt.Errorf("job config has empty work_subject")
		}
		if strings.TrimSpace(dirArg) != "" {
			scanDir = dirArg
		} else {
			scanDir = cfg.Source
		}
		if strings.TrimSpace(scanDir) == "" {
			return fmt.Errorf("no directory to scan: pass [directory] or define source in the sync job (nexus sync)")
		}
	} else {
		if strings.TrimSpace(dirArg) == "" {
			return fmt.Errorf("directory argument is required")
		}
		scanDir = dirArg
	}

	absDir, err := filepath.Abs(scanDir)
	if err != nil {
		return fmt.Errorf("abs path: %w", err)
	}

	start := time.Now()

	var objectCount, totalSize atomic.Int64

	jobs := make(chan FileEntry, workers*4)
	g, ctx := errgroup.WithContext(ctx)

	// results is non-nil only when output is needed (parquet or NATS).
	// It is always closed by the dedicated closer goroutine below,
	// which waits for all workers to finish via workerWg — no implicit
	// invariant between results and any other variable.
	var results chan FileRecord
	var workerWg sync.WaitGroup

	switch {
	case parquetOut != "":
		results = make(chan FileRecord, workers*4)
		g.Go(func() error {
			f, err := os.Create(parquetOut)
			if err != nil {
				return fmt.Errorf("create parquet file: %w", err)
			}
			defer f.Close()

			writer := parquet.NewGenericWriter[FileRecord](f)
			batch := make([]FileRecord, 0, parquetBatchRows)

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
				if len(batch) >= parquetBatchRows {
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

	case jobNatsMode:
		results = make(chan FileRecord, workers*4)
		g.Go(func() error {
			for rec := range results {
				data, err := json.Marshal(rec)
				if err != nil {
					return fmt.Errorf("marshal record: %w", err)
				}
				var pubErr error
				for attempt := range 5 {
					if _, pubErr = js.Publish(workSubject, data); pubErr == nil {
						break
					}
					if attempt < 4 {
						time.Sleep(time.Duration(1<<attempt) * 100 * time.Millisecond)
					}
				}
				if pubErr != nil {
					return fmt.Errorf("jetstream publish to %q: %w", workSubject, pubErr)
				}
			}
			return nil
		})
	}

	// Closer goroutine: always registered in the errgroup.
	// When results is nil it returns immediately; otherwise it waits for all
	// workers to finish before closing the channel so the sink goroutine above
	// can drain cleanly.
	//
	// workerWg tracks worker completion for channel lifecycle only; errors flow
	// through errgroup. The two mechanisms have non-overlapping responsibilities:
	// the closer cannot block on g.Wait() from inside the same group (deadlock),
	// so workerWg is the right tool here. Note that workerWg.Add(workers) is
	// called before the launch loop — never inside each goroutine — so the count
	// is already correct even if the closer's Wait() runs before a worker starts.
	g.Go(func() error {
		workerWg.Wait()
		if results != nil {
			close(results)
		}
		return nil
	})

	// Progress reporter runs outside the errgroup — it has no error to return
	// and must not block g.Wait(). stopProgress closes the channel exactly once
	// via sync.Once so the caller never panics regardless of the error path.
	scanDone := make(chan struct{})
	var stopOnce sync.Once
	stopProgress := func() { stopOnce.Do(func() { close(scanDone) }) }
	defer stopProgress() // safety net: always stop the reporter when runLs returns

	go func() {
		printProgress := func(t time.Time, rate, cpuPct float64) {
			line := fmt.Sprintf("\r\033[K[%s] %s objects",
				format.CompactDuration(t.Sub(start)),
				format.HumanSI(objectCount.Load()),
			)
			if all {
				line += fmt.Sprintf(" • %s", format.ByteSize(totalSize.Load()))
			}
			line += fmt.Sprintf(" • %s obj/s • cpu %.0f%% • rss %s",
				format.HumanSI(int64(rate)),
				cpuPct,
				format.HumanSI(osutil.RSS()),
			)
			fmt.Fprint(errOut, line)
		}
		printProgress(start, 0, 0)
		ticker := time.NewTicker(progressInterval)
		defer ticker.Stop()
		var prevCount, prevUser, prevSys int64
		prevTime := start
		for {
			select {
			case t := <-ticker.C:
				count := objectCount.Load()
				rate := float64(count-prevCount) / t.Sub(prevTime).Seconds()

				var ru syscall.Rusage
				syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
				curUser := ru.Utime.Sec*1_000_000 + int64(ru.Utime.Usec)
				curSys := ru.Stime.Sec*1_000_000 + int64(ru.Stime.Usec)
				cpuPct := float64((curUser-prevUser)+(curSys-prevSys)) / float64(t.Sub(prevTime).Microseconds()) * 100

				prevCount = count
				prevTime = t
				prevUser = curUser
				prevSys = curSys
				printProgress(t, rate, cpuPct)
			case <-scanDone:
				return
			}
		}
	}()

	// Workers: tracked by workerWg so the closer goroutine knows when to shut
	// down the results channel. They are also in the errgroup so any error
	// propagates to g.Wait().
	workerWg.Add(workers)
	for range workers {
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
							continue
						}
						st := info.Sys().(*syscall.Stat_t)
						rec.Inode = st.Ino
						rec.Size = info.Size()
						rec.MTime = info.ModTime().Unix()
						rec.CTime = st.Ctim.Sec
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
			entries, err := f.ReadDir(readDirChunk)
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

	waitErr := g.Wait()
	stopProgress() // stop the reporter as soon as all goroutines have finished

	if waitErr != nil && !errors.Is(waitErr, context.Canceled) {
		return waitErr
	}

	if jobNatsMode && ctx.Err() == nil && waitErr == nil {
		if _, err := kv.Put(kvKeyLsDone, []byte("true")); err != nil {
			return fmt.Errorf("broker %q: cannot notify job %q that listing finished: %w", natsURL, token, err)
		}
		fmt.Fprintf(errOut, "  ✔ Workers notified: file listing is complete\n")
	}

	fmt.Fprintf(errOut, "\r\033[K")
	if ctx.Err() != nil {
		fmt.Fprintf(out, "⚠ Scan interrupted\n\n")
	} else {
		fmt.Fprintf(out, "✔ Scan completed\n\n")
	}
	fmt.Fprintf(out, "  Objects scanned : %s\n", format.HumanSI(objectCount.Load()))
	if all {
		fmt.Fprintf(out, "  Total size    : %s\n", format.ByteSize(totalSize.Load()))
	}
	fmt.Fprintf(out, "  Duration      : %s\n", time.Since(start).Round(time.Millisecond))

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
