package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"nexus/internal/app"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/spf13/cobra"
)

const (
	kvKeyConfig = "config"

	kvTTL          = 24 * time.Hour
	workStreamTTL  = 24 * time.Hour
	statsStreamTTL = time.Hour

	// workMaxMsgs caps the work queue; when full, the producer blocks (backpressure).
	workMaxMsgs = 500_000
)

// SyncConfig is persisted for the job (NATS JetStream) under key kvKeyConfig.
type SyncConfig struct {
	JobID        string    `json:"job_id"`
	Source       string    `json:"source"`
	Destination  string    `json:"destination"`
	CreatedAt    time.Time `json:"created_at"`
	WorkSubject  string    `json:"work_subject"`
	StatsSubject string    `json:"stats_subject"`
}

func init() {
	RootCmd.AddCommand(newSyncCmd())
}

func newSyncCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "sync <source> <destination> <nats>",
		GroupID: groupCore,
		Short:   "Initialize a synchronization job",
		Args: func(cmd *cobra.Command, args []string) error {
			if err := cobra.ExactArgs(3)(cmd, args); err != nil {
				return fmt.Errorf("requires <source> <destination> <nats>")
			}
			if strings.TrimSpace(args[2]) == "" {
				return fmt.Errorf("<nats> must be a non-empty broker URL")
			}
			return nil
		},
		// cmd is no longer ignored: we forward cmd.Context() so the signal-aware
		// context installed by setupRootCommand propagates into runSync.
		RunE: func(cmd *cobra.Command, args []string) error {
			return runSync(cmd.Context(), args[0], args[1], strings.TrimSpace(args[2]))
		},
		Example: fmt.Sprintf(`  # Initialise a default synchronization job
  %s sync /path/to/source /path/to/destination nats://localhost:4222`,
			app.Name),
	}

	return cmd
}

// ctx is available for future use (graceful cancellation of long-running NATS ops).
func runSync(_ context.Context, src, dst, natsURL string) error {
	// Connect before allocating a job id so we never print a UUID that was never created.
	nc, err := nats.Connect(natsURL,
		nats.Name(app.Name+"-sync"),
		nats.Timeout(10*time.Second),
		nats.RetryOnFailedConnect(false),
		nats.MaxReconnects(0),
	)
	if err != nil {
		return fmt.Errorf("broker %q: cannot connect (check that NATS is running, the URL is correct, and no firewall blocks the port): %w", natsURL, err)
	}
	defer nc.Drain()

	js, err := nc.JetStream()
	if err != nil {
		return fmt.Errorf("broker %q: JetStream client unavailable: %w", natsURL, err)
	}

	if _, err := js.AccountInfo(); err != nil {
		return fmt.Errorf("broker %q: JetStream is not enabled or not reachable for this account (enable jetstream in nats-server.conf): %w", natsURL, err)
	}

	jobID := uuid.New().String()
	workStream := jobID + "-work"
	statsStream := jobID + "-stats"
	workSubject := jobID + ".object"
	statsSubject := jobID + ".statistics"

	const labelCol = 10
	fmt.Fprintf(os.Stderr, "%-*s %s\n", labelCol, "Job", jobID)
	fmt.Fprintf(os.Stderr, "%-*s %s\n", labelCol, "Source", src)
	fmt.Fprintf(os.Stderr, "%-*s %s\n", labelCol, "Target", dst)
	fmt.Fprintf(os.Stderr, "%-*s %s\n", labelCol, "Broker", natsURL)
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "Initializing NATS infrastructure...")
	fmt.Fprintln(os.Stderr)

	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket:      jobID,
		TTL:         kvTTL,
		Storage:     nats.FileStorage,
		Description: app.Name + " job " + jobID,
	})
	if err != nil {
		return fmt.Errorf("broker %q: cannot create job storage (NATS Server 2.6.2+ with JetStream required): %w", natsURL, err)
	}
	fmt.Fprintf(os.Stderr, "✔ %s\n", "Job storage ready")

	if err := createStreamIfNotExists(js, &nats.StreamConfig{
		Name:        workStream,
		Subjects:    []string{workSubject},
		Retention:   nats.WorkQueuePolicy,
		Storage:     nats.FileStorage,
		MaxAge:      workStreamTTL,
		MaxMsgs:     workMaxMsgs,
		Discard:     nats.DiscardOld,
		Description: app.Name + " work queue " + jobID,
	}); err != nil {
		return fmt.Errorf("broker %q: create work stream: %w", natsURL, err)
	}
	fmt.Fprintf(os.Stderr, "✔ %s\n", "Work stream ready")

	if err := createStreamIfNotExists(js, &nats.StreamConfig{
		Name:        statsStream,
		Subjects:    []string{statsSubject},
		Retention:   nats.InterestPolicy,
		Storage:     nats.MemoryStorage,
		MaxAge:      statsStreamTTL,
		Description: app.Name + " stats " + jobID,
	}); err != nil {
		return fmt.Errorf("broker %q: create stats stream: %w", natsURL, err)
	}
	fmt.Fprintf(os.Stderr, "✔ %s\n", "Stats stream ready")

	cfg := SyncConfig{
		JobID:        jobID,
		Source:       src,
		Destination:  dst,
		CreatedAt:    time.Now().UTC(),
		WorkSubject:  workSubject,
		StatsSubject: statsSubject,
	}

	data, err := json.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("marshal sync config: %w", err)
	}

	if _, err := kv.Put(kvKeyConfig, data); err != nil {
		return fmt.Errorf("broker %q: cannot save job settings: %w", natsURL, err)
	}
	fmt.Fprintf(os.Stderr, "✔ %s\n", "Configuration published")

	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "✓ Ready to sync")
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "Next steps:")
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "  Run workers:")
	fmt.Fprintf(os.Stderr, "    %s worker --token %s\n", app.Name, jobID)
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "  Monitor progress:")
	fmt.Fprintf(os.Stderr, "    %s status --token %s\n", app.Name, jobID)
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "  List files:")
	fmt.Fprintf(os.Stderr, "    %s ls --nats %q --token %s\n", app.Name, natsURL, jobID)
	fmt.Fprintf(os.Stderr, "    # optional: %s ls <dir> --nats %q --token %s  (override sync source)\n", app.Name, natsURL, jobID)
	fmt.Fprintln(os.Stderr)

	fmt.Println(jobID)
	return nil
}

func createStreamIfNotExists(js nats.JetStreamContext, cfg *nats.StreamConfig) error {
	_, err := js.StreamInfo(cfg.Name)
	if err == nil {
		return fmt.Errorf("stream %q already exists (job conflict?)", cfg.Name)
	}
	if err != nats.ErrStreamNotFound {
		return fmt.Errorf("stream info %q: %w", cfg.Name, err)
	}

	if _, err := js.AddStream(cfg); err != nil {
		return fmt.Errorf("add stream %q: %w", cfg.Name, err)
	}
	return nil
}
