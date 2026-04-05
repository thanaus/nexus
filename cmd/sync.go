package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
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
		RunE: func(cmd *cobra.Command, args []string) error {
			return runSync(cmd.Context(), args[0], args[1], strings.TrimSpace(args[2]), cmd.OutOrStdout(), cmd.ErrOrStderr())
		},
		Example: fmt.Sprintf(`  # Initialise a default synchronization job
  %s sync /path/to/source /path/to/destination nats://localhost:4222`,
			app.Name),
	}

	return cmd
}

// runSync initialises the NATS infrastructure for a sync job. ctx is the
// signal-aware context propagated by ExecuteContext; each major operation is
// guarded by a ctx.Err() check so that a SIGINT interrupts the setup cleanly.
// Human-readable output goes to errOut; the bare job ID goes to out so that
// callers can capture it with a pipe (e.g. TOKEN=$(nexus sync …)).
func runSync(ctx context.Context, src, dst, natsURL string, out, errOut io.Writer) error {
	if err := ctx.Err(); err != nil {
		return err
	}

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

	if err := ctx.Err(); err != nil {
		return err
	}

	js, err := nc.JetStream()
	if err != nil {
		return fmt.Errorf("broker %q: JetStream client unavailable: %w", natsURL, err)
	}

	if _, err := js.AccountInfo(); err != nil {
		return fmt.Errorf("broker %q: JetStream is not enabled or not reachable for this account (enable jetstream in nats-server.conf): %w", natsURL, err)
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	jobID := uuid.New().String()
	workStream := jobID + "-work"
	statsStream := jobID + "-stats"
	workSubject := jobID + ".object"
	statsSubject := jobID + ".statistics"

	const labelCol = 10
	fmt.Fprintf(errOut, "%-*s %s\n", labelCol, "Job", jobID)
	fmt.Fprintf(errOut, "%-*s %s\n", labelCol, "Source", src)
	fmt.Fprintf(errOut, "%-*s %s\n", labelCol, "Target", dst)
	fmt.Fprintf(errOut, "%-*s %s\n", labelCol, "Broker", natsURL)
	fmt.Fprintln(errOut)
	fmt.Fprintln(errOut, "Initializing NATS infrastructure...")
	fmt.Fprintln(errOut)

	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket:      jobID,
		TTL:         kvTTL,
		Storage:     nats.FileStorage,
		Description: app.Name + " job " + jobID,
	})
	if err != nil {
		return fmt.Errorf("broker %q: cannot create job storage (NATS Server 2.6.2+ with JetStream required): %w", natsURL, err)
	}
	fmt.Fprintf(errOut, "✔ %s\n", "Job storage ready")

	if err := ctx.Err(); err != nil {
		return err
	}

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
	fmt.Fprintf(errOut, "✔ %s\n", "Work stream ready")

	if err := ctx.Err(); err != nil {
		return err
	}

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
	fmt.Fprintf(errOut, "✔ %s\n", "Stats stream ready")

	if err := ctx.Err(); err != nil {
		return err
	}

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
	fmt.Fprintf(errOut, "✔ %s\n", "Configuration published")

	fmt.Fprintln(errOut)
	fmt.Fprintln(errOut, "✓ Ready to sync")
	fmt.Fprintln(errOut)
	fmt.Fprintln(errOut, "Next steps:")
	fmt.Fprintln(errOut)
	fmt.Fprintln(errOut, "  Run workers:")
	fmt.Fprintf(errOut, "    %s worker --token %s\n", app.Name, jobID)
	fmt.Fprintln(errOut)
	fmt.Fprintln(errOut, "  Monitor progress:")
	fmt.Fprintf(errOut, "    %s status --token %s\n", app.Name, jobID)
	fmt.Fprintln(errOut)
	fmt.Fprintln(errOut, "  List files:")
	fmt.Fprintf(errOut, "    %s ls --nats %q --token %s\n", app.Name, natsURL, jobID)
	fmt.Fprintf(errOut, "    # optional: %s ls <dir> --nats %q --token %s  (override sync source)\n", app.Name, natsURL, jobID)
	fmt.Fprintln(errOut)

	fmt.Fprintln(out, jobID)
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
