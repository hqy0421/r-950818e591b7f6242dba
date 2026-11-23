package flatjsonl

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/bool64/sqluct"
	_ "modernc.org/sqlite" // Database driver.
)

// SQLiteWriter inserts rows into SQLite database.
//
// Device models keep reporting new fields, so a flattened row can grow wider
// than the SQLite column limit (SQLMaxCols, 2000 by default). The writer
// splits every wide row into shard tables according to a deterministic column
// plan that is computed once from the ordered key list:
//
//	<table>        columns 1..maxCols
//	<table>_part2  columns maxCols+1..2*maxCols
//	<table>_part3  ...
//
// Every shard table carries the source row key _seq_id, so the original wide
// row can be reassembled one-to-one with a join on _seq_id. Field placement
// is documented in a readable catalog table <table>_columns.
//
// Three auxiliary tables are always created next to the data tables:
//
//	<table>_columns        column catalog: which table/column holds which source field
//	<table>_absent         per source row, a JSON array of column names that were missing in that row
//	<table>_duplicate_seq  source row keys that were delivered again and ignored (first delivery wins)
type SQLiteWriter struct {
	db        *sql.DB
	tableName string
	tx        *sql.Tx
	rowsTx    int
	sizeTx    int
	replacer  *strings.Replacer
	p         *Processor
	maxCols   int

	mainPlan         columnPlan
	transposedPlans  map[string]columnPlan
	sortedTransposed []string

	transposed map[string]*baseWriter
	b          *baseWriter
}

// columnShard is a single shard table of a column plan.
type columnShard struct {
	table string  // Table name.
	start int     // Index of the first shard column in the full column list.
	keys  []flKey // Columns of this shard, a sub-slice of the full column list.
}

// columnPlan is a deterministic assignment of wide-row columns to shard
// tables: it is a pure function of the ordered column list and maxCols, so
// the same input always produces the same shard layout, and both table
// creation and row insertion derive the split from this single source.
type columnPlan struct {
	shards []columnShard
}

// newColumnPlan splits keys into shards of at most maxCols columns each.
// The first shard keeps the base table name, following shards are suffixed
// with _part2, _part3, ... .
func newColumnPlan(table string, keys []flKey, maxCols int) columnPlan {
	if maxCols < 1 {
		maxCols = 1
	}

	var shards []columnShard

	for start := 0; start < len(keys); start += maxCols {
		end := start + maxCols
		if end > len(keys) {
			end = len(keys)
		}

		shardTable := table
		if len(shards) > 0 {
			shardTable = table + partSuffix + strconv.Itoa(len(shards)+1)
		}

		shards = append(shards, columnShard{
			table: shardTable,
			start: start,
			keys:  keys[start:end],
		})
	}

	if len(shards) == 0 {
		// Keep the row-key table even for a row without columns.
		shards = append(shards, columnShard{table: table})
	}

	return columnPlan{shards: shards}
}

// NewSQLiteWriter creates an instance of SQLiteWriter.
func NewSQLiteWriter(fn string, tableName string, p *Processor) (*SQLiteWriter, error) {
	var err error

	db := p.f.SQLiteInstance

	if db == nil {
		db, err = sql.Open("sqlite", fn)
		if err != nil {
			return nil, err
		}
	}

	_, err = db.Exec("pragma journal_mode=off;")
	if err != nil {
		return nil, err
	}

	if p.f.SQLMaxCols == 0 {
		p.f.SQLMaxCols = 2000
	}

	c := &SQLiteWriter{
		db:        db,
		tableName: tableName,
		replacer:  strings.NewReplacer(`"`, `""`),
		p:         p,
		maxCols:   p.f.SQLMaxCols - 1, // -1 for _seq_id.
	}

	return c, nil
}

// SetupKeys creates tables.
func (c *SQLiteWriter) SetupKeys(keys []flKey) error {
	c.b = &baseWriter{}
	c.b.p = c.p
	c.b.setupKeys(keys)
	c.transposed = map[string]*baseWriter{}
	c.transposedPlans = map[string]columnPlan{}

	c.mainPlan = newColumnPlan(c.tableName, c.b.filteredKeys, c.maxCols)

	if err := c.createShards(c.mainPlan, false); err != nil {
		return err
	}

	for dst, tw := range c.b.transposed {
		tw.extName = c.table(dst)

		plan := newColumnPlan(tw.extName, tw.filteredKeys, c.maxCols)
		if err := c.createShards(plan, true); err != nil {
			return err
		}

		c.transposed[dst] = tw
		c.transposedPlans[dst] = plan
		c.sortedTransposed = append(c.sortedTransposed, dst)
	}

	sort.Strings(c.sortedTransposed)

	if err := c.createMetaTables(); err != nil {
		return err
	}

	if err := c.writeCatalog(); err != nil {
		return err
	}

	switch {
	case len(keys) < 100:
		c.sizeTx = 10000
	case len(keys) < 1000:
		c.sizeTx = 1000
	default:
		c.sizeTx = 100
	}

	return nil
}

func (c *SQLiteWriter) table(dst string) string {
	if dst != "" {
		return c.tableName + "_" + dst
	}

	return c.tableName
}

// ReceiveRow receives rows.
func (c *SQLiteWriter) ReceiveRow(seq int64, values []Value) error {
	vv := c.b.receiveRowValues(values)

	dup, err := c.insertRow(seq, c.mainPlan, vv, true)
	if err != nil {
		return fmt.Errorf("writing SQLite row: %w", err)
	}

	if dup {
		// First delivery of this source row key won, ignore the rest of the row.
		if err := c.recordDuplicateSeq(seq); err != nil {
			return err
		}
	} else {
		if err := c.recordAbsent(seq, vv); err != nil {
			return err
		}

		for _, dst := range c.sortedTransposed {
			tw := c.transposed[dst]

			for _, r := range tw.receiveTransposedRowValues(seq, values) {
				if _, err := c.insertRow(seq, c.transposedPlans[dst], r, false); err != nil {
					return fmt.Errorf("transposed rows for %s: %w", dst, err)
				}
			}
		}
	}

	if c.rowsTx >= c.sizeTx {
		if err := c.commitTx(); err != nil {
			return fmt.Errorf("committing tx: %w", err)
		}
	}

	return nil
}

// insertRow inserts a wide row into the shard tables of a column plan.
//
// If detectDup is set, the first shard is inserted with INSERT OR IGNORE and
// a conflicting source row key (duplicate seq) makes the function return true
// without touching the remaining shards: the first delivery of a source row
// wins, later deliveries of the same _seq_id are ignored.
func (c *SQLiteWriter) insertRow(seq int64, plan columnPlan, values []Value, detectDup bool) (bool, error) {
	for i, shard := range plan.shards {
		end := shard.start + len(shard.keys)
		if end > len(values) {
			end = len(values)
		}

		orIgnore := ""
		if detectDup && i == 0 {
			orIgnore = "OR IGNORE "
		}

		res := `INSERT ` + orIgnore + `INTO "` + shard.table + `" VALUES (` + strconv.Itoa(int(seq))

		for _, v := range values[shard.start:end] {
			if v.Type != TypeNull && v.Type != TypeAbsent {
				res += `,"` + c.replacer.Replace(v.Format()) + `"`
			} else {
				res += `,NULL`
			}
		}

		res += `)`

		r, err := c.execTx(res)
		if err != nil {
			return false, err
		}

		if detectDup && i == 0 {
			affected, err := r.RowsAffected()
			if err != nil {
				return false, fmt.Errorf("failed to check duplicate source row key: %w", err)
			}

			if affected == 0 {
				return true, nil
			}
		}
	}

	return false, nil
}

// recordAbsent stores the list of missing (absent) fields of a source row, so
// that a reassembled row can tell "field was not reported" (listed here, NULL
// in the data tables) from "field was reported as JSON null" (not listed
// here, NULL in the data tables).
func (c *SQLiteWriter) recordAbsent(seq int64, values []Value) error {
	var absent []string

	for i, v := range values {
		if v.Type == TypeAbsent {
			absent = append(absent, c.b.filteredKeys[i].replaced)
		}
	}

	if len(absent) == 0 {
		return nil
	}

	fields, err := json.Marshal(absent)
	if err != nil {
		return fmt.Errorf("failed to marshal absent fields: %w", err)
	}

	if _, err := c.execTx(`INSERT INTO "`+c.tableName+`_absent" VALUES (?, ?)`, seq, string(fields)); err != nil {
		return fmt.Errorf("failed to record absent fields of seq %d: %w", seq, err)
	}

	return nil
}

// recordDuplicateSeq registers an ignored duplicate delivery of a source row
// key: one row per duplicate, so SELECT _seq_id, COUNT(*) gives per-key counts.
func (c *SQLiteWriter) recordDuplicateSeq(seq int64) error {
	if _, err := c.execTx(`INSERT INTO "`+c.tableName+`_duplicate_seq" VALUES (?)`, seq); err != nil {
		return fmt.Errorf("failed to record duplicate seq %d: %w", seq, err)
	}

	return nil
}

func (c *SQLiteWriter) commitTx() error {
	if err := c.tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit SQLite transaction: %w", err)
	}

	c.tx = nil
	c.rowsTx = 0

	return nil
}

func (c *SQLiteWriter) execTx(query string, args ...any) (sql.Result, error) {
	if c.tx == nil {
		tx, err := c.db.Begin()
		if err != nil {
			return nil, fmt.Errorf("failed to begin SQLite transaction: %w", err)
		}

		c.tx = tx
		c.rowsTx = 0
	}

	c.rowsTx++

	r, err := c.tx.Exec(query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to insert SQLite row: %w", err)
	}

	return r, nil
}

// createShards creates the shard tables of a column plan. Every shard table
// starts with the _seq_id source row key.
func (c *SQLiteWriter) createShards(plan columnPlan, isTransposed bool) error {
	rowKey := "_seq_id integer primary key"
	if isTransposed {
		rowKey = "_seq_id integer"
	}

	for _, shard := range plan.shards {
		createTable := `CREATE TABLE "` + shard.table + `" (` + "\n\t" + rowKey + ",\n"

		for _, k := range shard.keys {
			tp := ""

			switch k.t { //nolint: exhaustive
			case TypeInt, TypeBool:
				tp = " INTEGER"
			case TypeFloat:
				tp = " REAL"
			}

			createTable += sqluct.QuoteRequiredBackticks(k.replaced) + tp + ",\n"
		}

		createTable = createTable[:len(createTable)-2] + "\n)"

		if _, err := c.db.Exec(createTable); err != nil {
			return fmt.Errorf("failed to create SQLite table %q with %d keys: %w", shard.table, len(shard.keys), err)
		}
	}

	return nil
}

// createMetaTables creates the column catalog, the absent-fields table and
// the duplicate-seq table.
func (c *SQLiteWriter) createMetaTables() error {
	tables := []string{
		// Column catalog: where every source field landed. column_ordinal is
		// the 0-based column position in the shard table (matches the cid of
		// pragma table_info), role is 'row_key' or 'data'.
		`CREATE TABLE "` + c.tableName + `_columns" (
	table_name TEXT NOT NULL,
	column_name TEXT NOT NULL,
	column_ordinal INTEGER NOT NULL,
	source_key TEXT NOT NULL,
	value_type TEXT NOT NULL,
	role TEXT NOT NULL
)`,
		// Missing fields of a source row: fields is a JSON array of column
		// names that were absent in the source row. For these columns NULL in
		// the data tables means "not reported", for the rest it means
		// "reported as JSON null".
		`CREATE TABLE "` + c.tableName + `_absent" (
	_seq_id INTEGER PRIMARY KEY,
	fields TEXT NOT NULL
)`,
		// Source row keys that were delivered more than once: the first
		// delivery is stored in the data tables, every further delivery adds
		// one row here and is otherwise ignored.
		`CREATE TABLE "` + c.tableName + `_duplicate_seq" (
	_seq_id INTEGER NOT NULL
)`,
	}

	for _, q := range tables {
		if _, err := c.db.Exec(q); err != nil {
			return fmt.Errorf("failed to create SQLite meta table: %w", err)
		}
	}

	return nil
}

// writeCatalog fills the <table>_columns catalog with the column plans of the
// main table and all transposed tables.
func (c *SQLiteWriter) writeCatalog() error {
	tx, err := c.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin SQLite transaction: %w", err)
	}

	writePlan := func(plan columnPlan) error {
		for _, shard := range plan.shards {
			if _, err := tx.Exec(`INSERT INTO "`+c.tableName+`_columns" VALUES (?, ?, ?, ?, ?, ?)`,
				shard.table, "_seq_id", 0, "_seq_id", "integer", "row_key"); err != nil {
				return err
			}

			for i, k := range shard.keys {
				typ := string(k.t)
				if typ == "" {
					typ = "any"
				}

				if _, err := tx.Exec(`INSERT INTO "`+c.tableName+`_columns" VALUES (?, ?, ?, ?, ?, ?)`,
					shard.table, k.replaced, i+1, k.original, typ, "data"); err != nil {
					return err
				}
			}
		}

		return nil
	}

	if err := writePlan(c.mainPlan); err != nil {
		return fmt.Errorf("failed to write SQLite column catalog: %w", err)
	}

	for _, dst := range c.sortedTransposed {
		if err := writePlan(c.transposedPlans[dst]); err != nil {
			return fmt.Errorf("failed to write SQLite column catalog: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit SQLite column catalog: %w", err)
	}

	return nil
}

// Close commits outstanding transaction and closes database instance.
func (c *SQLiteWriter) Close() error {
	if c.tx != nil {
		err := c.tx.Commit()
		if err != nil {
			println(err.Error())
		}
	}

	return c.db.Close()
}
