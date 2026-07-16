package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/bjaus/flow/app/internal/core"
	_ "modernc.org/sqlite"
)

var ErrNotFound = errors.New("not found")

type SQLiteStore struct {
	db *sql.DB

	mu     sync.Mutex
	nextID uint64
	subs   map[uint64]*subscriber
}

type subscriber struct {
	runID string
	ch    chan core.Event
}

func OpenSQLite(path string) (*SQLiteStore, error) {
	if path == "" {
		path = "flow.db"
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err = db.ExecContext(ctx, `PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON; PRAGMA busy_timeout=5000;`); err != nil {
		db.Close()
		return nil, fmt.Errorf("configure sqlite: %w", err)
	}
	if err = migrate(ctx, db); err != nil {
		db.Close()
		return nil, err
	}
	return &SQLiteStore{db: db, subs: make(map[uint64]*subscriber)}, nil
}

func migrate(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS checkpoints (
  id TEXT PRIMARY KEY,
  data BLOB NOT NULL,
  updated_at TIMESTAMP NOT NULL
);
CREATE TABLE IF NOT EXISTS runs (
  id TEXT PRIMARY KEY,
  workflow TEXT NOT NULL,
  fingerprint TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL,
  input BLOB NOT NULL,
  result BLOB,
  error TEXT NOT NULL DEFAULT '',
  interrupt_id TEXT NOT NULL DEFAULT '',
  gate_prompt TEXT NOT NULL DEFAULT '',
  decision BLOB,
  cancel_pending INTEGER NOT NULL DEFAULT 0,
  created_at TIMESTAMP NOT NULL,
  started_at TIMESTAMP,
  finished_at TIMESTAMP,
  updated_at TIMESTAMP NOT NULL
);
CREATE INDEX IF NOT EXISTS runs_queue ON runs(status, created_at, id);
CREATE INDEX IF NOT EXISTS runs_workflow ON runs(workflow, status);
CREATE TABLE IF NOT EXISTS events (
  run_id TEXT NOT NULL,
  seq INTEGER NOT NULL,
  kind TEXT NOT NULL,
  at TIMESTAMP NOT NULL,
  data BLOB,
  PRIMARY KEY(run_id, seq)
);
CREATE INDEX IF NOT EXISTS events_at ON events(at, run_id, seq);
`)
	if err != nil {
		return fmt.Errorf("migrate sqlite: %w", err)
	}
	return nil
}

func (s *SQLiteStore) Close() error                   { return s.db.Close() }
func (s *SQLiteStore) Ping(ctx context.Context) error { return s.db.PingContext(ctx) }

func (s *SQLiteStore) Get(ctx context.Context, id string) ([]byte, bool, error) {
	var b []byte
	err := s.db.QueryRowContext(ctx, `SELECT data FROM checkpoints WHERE id=?`, id).Scan(&b)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("get checkpoint: %w", err)
	}
	return append([]byte(nil), b...), true, nil
}

func (s *SQLiteStore) Set(ctx context.Context, id string, data []byte) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO checkpoints(id,data,updated_at) VALUES(?,?,?) ON CONFLICT(id) DO UPDATE SET data=excluded.data,updated_at=excluded.updated_at`, id, data, time.Now().UTC())
	if err != nil {
		return fmt.Errorf("set checkpoint: %w", err)
	}
	return nil
}

func (s *SQLiteStore) Delete(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM checkpoints WHERE id=?`, id)
	if err != nil {
		return fmt.Errorf("delete checkpoint: %w", err)
	}
	return nil
}

func (s *SQLiteStore) Save(ctx context.Context, r *core.Run) error {
	if r == nil || r.ID == "" {
		return errors.New("save run: id is required")
	}
	now := time.Now().UTC()
	if r.CreatedAt.IsZero() {
		r.CreatedAt = now
	}
	r.UpdatedAt = now
	var decision []byte
	if r.Decision != nil {
		decision, _ = json.Marshal(r.Decision)
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO runs(id,workflow,fingerprint,status,input,result,error,interrupt_id,gate_prompt,decision,cancel_pending,created_at,started_at,finished_at,updated_at)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET workflow=excluded.workflow,fingerprint=excluded.fingerprint,status=excluded.status,input=excluded.input,result=excluded.result,error=excluded.error,interrupt_id=excluded.interrupt_id,gate_prompt=excluded.gate_prompt,decision=excluded.decision,cancel_pending=excluded.cancel_pending,started_at=excluded.started_at,finished_at=excluded.finished_at,updated_at=excluded.updated_at`,
		r.ID, r.Workflow, r.Fingerprint, r.Status, []byte(r.Input), []byte(r.Result), r.Error, r.InterruptID, r.GatePrompt, decision, r.CancelPending, r.CreatedAt, r.StartedAt, r.FinishedAt, r.UpdatedAt)
	if err != nil {
		return fmt.Errorf("save run: %w", err)
	}
	return nil
}

const runColumns = `id,workflow,fingerprint,status,input,result,error,interrupt_id,gate_prompt,decision,cancel_pending,created_at,started_at,finished_at,updated_at`

type scanner interface{ Scan(...any) error }

func scanRun(row scanner) (*core.Run, error) {
	r := &core.Run{}
	var input, result, decision []byte
	if err := row.Scan(&r.ID, &r.Workflow, &r.Fingerprint, &r.Status, &input, &result, &r.Error, &r.InterruptID, &r.GatePrompt, &decision, &r.CancelPending, &r.CreatedAt, &r.StartedAt, &r.FinishedAt, &r.UpdatedAt); err != nil {
		return nil, err
	}
	r.Input, r.Result = append([]byte(nil), input...), append([]byte(nil), result...)
	if len(decision) > 0 {
		r.Decision = &core.Decision{}
		if err := json.Unmarshal(decision, r.Decision); err != nil {
			return nil, fmt.Errorf("decode decision: %w", err)
		}
	}
	return r, nil
}

func (s *SQLiteStore) GetRun(ctx context.Context, id string) (*core.Run, error) {
	r, err := scanRun(s.db.QueryRowContext(ctx, `SELECT `+runColumns+` FROM runs WHERE id=?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get run: %w", err)
	}
	return r, nil
}

// Get cannot implement both checkpoint and run interfaces because Go has no overload. RunStore is exposed
// through Runs(), while SQLiteStore itself remains the checkpoint store.
type Runs struct{ s *SQLiteStore }

func (s *SQLiteStore) Runs() *Runs                                    { return &Runs{s: s} }
func (r *Runs) Save(ctx context.Context, run *core.Run) error         { return r.s.Save(ctx, run) }
func (r *Runs) Get(ctx context.Context, id string) (*core.Run, error) { return r.s.GetRun(ctx, id) }

func (r *Runs) List(ctx context.Context, f core.RunFilter) ([]*core.Run, error) {
	q := `SELECT ` + runColumns + ` FROM runs WHERE 1=1`
	args := make([]any, 0, 2)
	if f.Workflow != "" {
		q += ` AND workflow=?`
		args = append(args, f.Workflow)
	}
	if f.Status != "" {
		q += ` AND status=?`
		args = append(args, f.Status)
	}
	q += ` ORDER BY created_at,id`
	rows, err := r.s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("list runs: %w", err)
	}
	defer rows.Close()
	var out []*core.Run
	for rows.Next() {
		run, err := scanRun(rows)
		if err != nil {
			return nil, fmt.Errorf("scan run: %w", err)
		}
		out = append(out, run)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list runs: %w", err)
	}
	return out, nil
}

func (r *Runs) Claim(ctx context.Context) (*core.Run, error) {
	tx, err := r.s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("claim begin: %w", err)
	}
	defer tx.Rollback()
	run, err := scanRun(tx.QueryRowContext(ctx, `SELECT `+runColumns+` FROM runs WHERE status IN (?,?) ORDER BY created_at,id LIMIT 1`, core.StatusQueued, core.StatusParked))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("claim select: %w", err)
	}
	now := time.Now().UTC()
	res, err := tx.ExecContext(ctx, `UPDATE runs SET status=?,started_at=COALESCE(started_at,?),updated_at=? WHERE id=? AND status IN (?,?)`, core.StatusRunning, now, now, run.ID, core.StatusQueued, core.StatusParked)
	if err != nil {
		return nil, fmt.Errorf("claim update: %w", err)
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return nil, nil
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("claim commit: %w", err)
	}
	run.Status, run.UpdatedAt = core.StatusRunning, now
	if run.StartedAt == nil {
		run.StartedAt = &now
	}
	return run, nil
}

func (s *SQLiteStore) Publish(e core.Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e.At.IsZero() {
		e.At = time.Now().UTC()
	}
	if e.Seq == 0 {
		if err := s.db.QueryRow(`SELECT COALESCE(MAX(seq),0)+1 FROM events WHERE run_id=?`, e.RunID).Scan(&e.Seq); err != nil {
			return
		}
	}
	if _, err := s.db.Exec(`INSERT OR IGNORE INTO events(run_id,seq,kind,at,data) VALUES(?,?,?,?,?)`, e.RunID, e.Seq, e.Kind, e.At, []byte(e.Data)); err != nil {
		return
	}
	for _, sub := range s.subs {
		if sub.runID == "" || sub.runID == e.RunID {
			select {
			case sub.ch <- e:
			default:
			}
		}
	}
}

func (s *SQLiteStore) Subscribe(runID string) (<-chan core.Event, func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ch := make(chan core.Event, 256)
	q := `SELECT run_id,seq,kind,at,data FROM events`
	var rows *sql.Rows
	var err error
	if runID == "" {
		rows, err = s.db.Query(q + ` ORDER BY at,run_id,seq`)
	} else {
		rows, err = s.db.Query(q+` WHERE run_id=? ORDER BY seq`, runID)
	}
	if err == nil {
		for rows.Next() {
			var e core.Event
			var data []byte
			if rows.Scan(&e.RunID, &e.Seq, &e.Kind, &e.At, &data) == nil {
				e.Data = append([]byte(nil), data...)
				ch <- e
			}
		}
		rows.Close()
	}
	s.nextID++
	id := s.nextID
	s.subs[id] = &subscriber{runID: runID, ch: ch}
	var once sync.Once
	return ch, func() { once.Do(func() { s.mu.Lock(); delete(s.subs, id); close(ch); s.mu.Unlock() }) }
}
