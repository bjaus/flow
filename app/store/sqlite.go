package store

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
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
	seen  map[string]int64
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
		_ = db.Close()
		return nil, fmt.Errorf("configure sqlite: %w", err)
	}
	if err = migrate(ctx, db); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &SQLiteStore{db: db, subs: make(map[uint64]*subscriber)}, nil
}

//go:embed schema.sql
var schema string

func migrate(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, schema); err != nil {
		return fmt.Errorf("migrate sqlite: %w", err)
	}
	for _, alter := range []string{
		`ALTER TABLE runs ADD COLUMN trigger_name TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE runs ADD COLUMN parent_id TEXT NOT NULL DEFAULT ''`,
	} {
		if _, err := db.ExecContext(ctx, alter); err != nil && !strings.Contains(err.Error(), "duplicate column") {
			return fmt.Errorf("migrate sqlite: %w", err)
		}
	}
	return nil
}

func (s *SQLiteStore) Close() error {
	s.mu.Lock()
	for id, sub := range s.subs {
		delete(s.subs, id)
		close(sub.ch)
	}
	s.mu.Unlock()
	return s.db.Close()
}
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
	_, err := s.db.ExecContext(ctx, `INSERT INTO runs(id,workflow,fingerprint,status,trigger_name,parent_id,input,result,error,interrupt_id,gate_prompt,decision,cancel_pending,created_at,started_at,finished_at,updated_at)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET workflow=excluded.workflow,fingerprint=excluded.fingerprint,status=excluded.status,trigger_name=excluded.trigger_name,parent_id=excluded.parent_id,input=excluded.input,result=excluded.result,error=excluded.error,interrupt_id=excluded.interrupt_id,gate_prompt=excluded.gate_prompt,decision=excluded.decision,cancel_pending=excluded.cancel_pending,started_at=excluded.started_at,finished_at=excluded.finished_at,updated_at=excluded.updated_at`,
		r.ID, r.Workflow, r.Fingerprint, r.Status, r.Trigger, r.ParentID, []byte(r.Input), []byte(r.Result), r.Error, r.InterruptID, r.GatePrompt, decision, r.CancelPending, r.CreatedAt, r.StartedAt, r.FinishedAt, r.UpdatedAt)
	if err != nil {
		return fmt.Errorf("save run: %w", err)
	}
	return nil
}

const runColumns = `id,workflow,fingerprint,status,trigger_name,parent_id,input,result,error,interrupt_id,gate_prompt,decision,cancel_pending,created_at,started_at,finished_at,updated_at`

type scanner interface{ Scan(...any) error }

func scanRun(row scanner) (*core.Run, error) {
	r := &core.Run{}
	var input, result, decision []byte
	if err := row.Scan(&r.ID, &r.Workflow, &r.Fingerprint, &r.Status, &r.Trigger, &r.ParentID, &input, &result, &r.Error, &r.InterruptID, &r.GatePrompt, &decision, &r.CancelPending, &r.CreatedAt, &r.StartedAt, &r.FinishedAt, &r.UpdatedAt); err != nil {
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
	if f.ParentID != "" {
		q += ` AND parent_id=?`
		args = append(args, f.ParentID)
	}
	q += ` ORDER BY created_at,id`
	rows, err := r.s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("list runs: %w", err)
	}
	defer func() { _ = rows.Close() }()
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
	return r.claim(ctx, `SELECT `+runColumns+` FROM runs WHERE status=? ORDER BY created_at,id LIMIT 1`, core.StatusQueued)
}

// ClaimByID claims one specific queued run, returning nil when it is not
// queued. It lets a parent run drive a spawned child inline (app.Spawner).
func (r *Runs) ClaimByID(ctx context.Context, id string) (*core.Run, error) {
	return r.claim(ctx, `SELECT `+runColumns+` FROM runs WHERE id=? AND status=?`, id, core.StatusQueued)
}

func (r *Runs) claim(ctx context.Context, query string, args ...any) (*core.Run, error) {
	tx, err := r.s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("claim begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	run, err := scanRun(tx.QueryRowContext(ctx, query, args...))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("claim select: %w", err)
	}
	now := time.Now().UTC()
	res, err := tx.ExecContext(ctx, `UPDATE runs SET status=?,started_at=COALESCE(started_at,?),updated_at=? WHERE id=? AND status=?`, core.StatusRunning, now, now, run.ID, core.StatusQueued)
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
		err := s.db.QueryRowContext(context.Background(), `INSERT INTO events(run_id,seq,kind,at,data) VALUES(?,(SELECT COALESCE(MAX(seq),0)+1 FROM events WHERE run_id=?),?,?,?) RETURNING seq`, e.RunID, e.RunID, e.Kind, e.At, []byte(e.Data)).Scan(&e.Seq)
		if err != nil {
			return
		}
	} else {
		if _, err := s.db.ExecContext(context.Background(), `INSERT OR IGNORE INTO events(run_id,seq,kind,at,data) VALUES(?,?,?,?,?)`, e.RunID, e.Seq, e.Kind, e.At, []byte(e.Data)); err != nil {
			return
		}
	}
	for _, sub := range s.subs {
		if (sub.runID == "" || sub.runID == e.RunID) && e.Seq > sub.seen[e.RunID] {
			sub.seen[e.RunID] = e.Seq
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
	q := `SELECT run_id,seq,kind,at,data FROM events`
	var rows *sql.Rows
	var err error
	if runID == "" {
		rows, err = s.db.Query(q + ` ORDER BY at,run_id,seq`)
	} else {
		rows, err = s.db.Query(q+` WHERE run_id=? ORDER BY seq`, runID)
	}
	var history []core.Event
	if err == nil {
		for rows.Next() {
			var e core.Event
			var data []byte
			if rows.Scan(&e.RunID, &e.Seq, &e.Kind, &e.At, &data) == nil {
				e.Data = append([]byte(nil), data...)
				history = append(history, e)
			}
		}
		_ = rows.Close()
	}
	ch := make(chan core.Event, len(history)+256)
	seen := make(map[string]int64)
	for _, e := range history {
		ch <- e
		if e.Seq > seen[e.RunID] {
			seen[e.RunID] = e.Seq
		}
	}
	s.nextID++
	id := s.nextID
	s.subs[id] = &subscriber{runID: runID, ch: ch, seen: seen}
	go s.pollSubscriber(id)
	var once sync.Once
	return ch, func() {
		once.Do(func() {
			s.mu.Lock()
			if _, ok := s.subs[id]; ok {
				delete(s.subs, id)
				close(ch)
			}
			s.mu.Unlock()
		})
	}
}

func (s *SQLiteStore) pollSubscriber(id uint64) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for range ticker.C {
		s.mu.Lock()
		sub := s.subs[id]
		if sub == nil {
			s.mu.Unlock()
			return
		}
		runID := sub.runID
		s.mu.Unlock()
		q := `SELECT run_id,seq,kind,at,data FROM events`
		args := []any{}
		if runID != "" {
			q += ` WHERE run_id=?`
			args = append(args, runID)
		}
		q += ` ORDER BY at,run_id,seq`
		rows, err := s.db.QueryContext(context.Background(), q, args...)
		if err != nil {
			continue
		}
		var events []core.Event
		for rows.Next() {
			var e core.Event
			var data []byte
			if rows.Scan(&e.RunID, &e.Seq, &e.Kind, &e.At, &data) == nil {
				e.Data = append([]byte(nil), data...)
				events = append(events, e)
			}
		}
		_ = rows.Close()
		s.mu.Lock()
		sub = s.subs[id]
		if sub == nil {
			s.mu.Unlock()
			return
		}
		for _, e := range events {
			if e.Seq > sub.seen[e.RunID] {
				sub.seen[e.RunID] = e.Seq
				select {
				case sub.ch <- e:
				default:
				}
			}
		}
		s.mu.Unlock()
	}
}
