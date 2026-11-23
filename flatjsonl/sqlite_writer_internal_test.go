package flatjsonl

import (
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestColumnPlan_deterministicShards(t *testing.T) {
	keys := make([]flKey, 7)
	for i := range keys {
		keys[i] = flKey{replaced: "k" + strconv.Itoa(i)}
	}

	p1 := newColumnPlan("t", keys, 3)
	p2 := newColumnPlan("t", keys, 3)

	// Same input always produces the same plan.
	assert.Equal(t, p1, p2)

	require.Len(t, p1.shards, 3)

	assert.Equal(t, "t", p1.shards[0].table)
	assert.Equal(t, "t_part2", p1.shards[1].table)
	assert.Equal(t, "t_part3", p1.shards[2].table)

	assert.Equal(t, 0, p1.shards[0].start)
	assert.Equal(t, 3, p1.shards[1].start)
	assert.Equal(t, 6, p1.shards[2].start)

	assert.Len(t, p1.shards[0].keys, 3)
	assert.Len(t, p1.shards[1].keys, 3)
	assert.Len(t, p1.shards[2].keys, 1)

	// Exact fit does not produce an empty trailing shard.
	p3 := newColumnPlan("t", keys[:6], 3)
	require.Len(t, p3.shards, 2)

	// Degenerate maxCols is guarded, no division by zero.
	p4 := newColumnPlan("t", keys, 0)
	require.Len(t, p4.shards, 7)

	// No keys still yields the row-key table.
	p5 := newColumnPlan("t", nil, 3)
	require.Len(t, p5.shards, 1)
	assert.Equal(t, "t", p5.shards[0].table)
	assert.Empty(t, p5.shards[0].keys)
}

func TestSQLiteWriter_shardCatalogAbsentDup(t *testing.T) {
	fn := filepath.Join(t.TempDir(), "wide.sqlite")

	p := &Processor{}
	p.f.SQLMaxCols = 4 // 3 data columns per shard table.

	w, err := NewSQLiteWriter(fn, "dev", p)
	require.NoError(t, err)

	keys := []flKey{
		{original: ".a", replaced: "a", t: TypeInt},
		{original: ".b", replaced: "b", t: TypeString},
		{original: ".c", replaced: "c", t: TypeFloat},
		{original: ".d", replaced: "d", t: TypeString},
		{original: ".e", replaced: "e", t: TypeInt},
		{original: ".f", replaced: "f", t: TypeString},
		{original: ".g", replaced: "g", t: TypeFloat},
	}

	require.NoError(t, w.SetupKeys(keys))

	num := func(raw string) Value { return Value{Type: TypeFloat, RawNumber: raw} }
	str := func(s string) Value { return Value{Type: TypeString, String: s} }

	// Full row.
	require.NoError(t, w.ReceiveRow(1, []Value{num("1"), str("x"), num("1.5"), str("y"), num("2"), str("z"), num("2.5")}))

	// Sparse row: b and e are not reported, c and g are reported as JSON null.
	require.NoError(t, w.ReceiveRow(2, []Value{num("3"), {Type: TypeAbsent}, {Type: TypeNull}, str("w"), {Type: TypeAbsent}, str("v"), {Type: TypeNull}}))

	// Duplicate deliveries of seq 2 must be ignored (first wins) and recorded.
	require.NoError(t, w.ReceiveRow(2, []Value{num("30"), str("X"), num("3.5"), str("Y"), num("20"), str("Z"), num("3.6")}))
	require.NoError(t, w.ReceiveRow(2, []Value{num("300"), str("XX"), num("4.5"), str("YY"), num("200"), str("ZZ"), num("4.6")}))

	// Row with a single reported field.
	require.NoError(t, w.ReceiveRow(3, []Value{num("4"), {Type: TypeAbsent}, {Type: TypeAbsent}, {Type: TypeAbsent}, {Type: TypeAbsent}, {Type: TypeAbsent}, {Type: TypeAbsent}}))

	require.NoError(t, w.Close())

	db, err := sql.Open("sqlite", fn)
	require.NoError(t, err)

	defer func() { require.NoError(t, db.Close()) }()

	queryAll := func(q string, args ...any) [][]any {
		rows, err := db.Query(q, args...)
		require.NoError(t, err)

		defer func() { require.NoError(t, rows.Close()) }()

		cols, err := rows.Columns()
		require.NoError(t, err)

		var res [][]any

		for rows.Next() {
			r := make([]any, len(cols))
			ptr := make([]any, len(cols))

			for i := range r {
				ptr[i] = &r[i]
			}

			require.NoError(t, rows.Scan(ptr...))

			res = append(res, r)
		}

		require.NoError(t, rows.Err())

		return res
	}

	// Wide row is split into shard tables, meta tables are created.
	tables := queryAll(`SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name`)
	assert.Equal(t, [][]any{
		{"dev"}, {"dev_absent"}, {"dev_columns"}, {"dev_duplicate_seq"}, {"dev_part2"}, {"dev_part3"},
	}, tables)

	// Every shard table carries the source row key, one-to-one reassembly by join.
	reassembled := queryAll(`SELECT t1.a, t1.b, t1.c, t2.d, t2.e, t2.f, t3.g
		FROM dev t1
		JOIN dev_part2 t2 ON t2._seq_id = t1._seq_id
		JOIN dev_part3 t3 ON t3._seq_id = t1._seq_id
		ORDER BY t1._seq_id`)
	assert.Equal(t, [][]any{
		{int64(1), "x", 1.5, "y", int64(2), "z", 2.5},
		{int64(3), nil, nil, "w", nil, "v", nil},
		{int64(4), nil, nil, nil, nil, nil, nil},
	}, reassembled)

	// Missing fields are listed explicitly: absent (not reported) fields of
	// seq 2 and 3, while JSON nulls (c, g of seq 2) are not listed.
	absentRows := queryAll(`SELECT _seq_id, fields FROM dev_absent ORDER BY _seq_id`)
	require.Len(t, absentRows, 2)

	assert.Equal(t, int64(2), absentRows[0][0])
	assert.Equal(t, int64(3), absentRows[1][0])

	var absent []string
	require.NoError(t, json.Unmarshal([]byte(absentRows[0][1].(string)), &absent))
	assert.Equal(t, []string{"b", "e"}, absent)

	require.NoError(t, json.Unmarshal([]byte(absentRows[1][1].(string)), &absent))
	assert.Equal(t, []string{"b", "c", "d", "e", "f", "g"}, absent)

	// Duplicate deliveries are recorded once per occurrence, first row wins.
	dups := queryAll(`SELECT _seq_id, COUNT(*) FROM dev_duplicate_seq GROUP BY _seq_id`)
	assert.Equal(t, [][]any{{int64(2), int64(2)}}, dups)

	firstWins := queryAll(`SELECT a FROM dev WHERE _seq_id = 2`)
	assert.Equal(t, [][]any{{int64(3)}}, firstWins)

	// Column catalog explains field placement deterministically.
	catalog := queryAll(`SELECT table_name, column_name, column_ordinal, source_key, value_type, role
		FROM dev_columns ORDER BY table_name, column_ordinal`)
	assert.Equal(t, [][]any{
		{"dev", "_seq_id", int64(0), "_seq_id", "integer", "row_key"},
		{"dev", "a", int64(1), ".a", "int", "data"},
		{"dev", "b", int64(2), ".b", "string", "data"},
		{"dev", "c", int64(3), ".c", "float", "data"},
		{"dev_part2", "_seq_id", int64(0), "_seq_id", "integer", "row_key"},
		{"dev_part2", "d", int64(1), ".d", "string", "data"},
		{"dev_part2", "e", int64(2), ".e", "int", "data"},
		{"dev_part2", "f", int64(3), ".f", "string", "data"},
		{"dev_part3", "_seq_id", int64(0), "_seq_id", "integer", "row_key"},
		{"dev_part3", "g", int64(1), ".g", "float", "data"},
	}, catalog)
}

func TestSQLiteWriter_endToEnd(t *testing.T) {
	dir := t.TempDir()
	input := filepath.Join(dir, "in.jsonl")
	out := filepath.Join(dir, "out.sqlite")

	require.NoError(t, os.WriteFile(input, []byte(`{"a":1,"b":"x","c":1.5,"d":"y","e":2}
{"a":3,"c":null,"d":"w"}
{"a":4,"b":"z","e":5}
`), 0o600))

	f := Flags{}
	f.Input = input
	f.SQLite = out
	f.SQLTable = "t"
	f.SQLMaxCols = 3 // 2 data columns per shard table.
	f.ReplaceKeys = true
	f.Concurrency = 1

	proc, err := NewProcessor(f, Config{}, f.Inputs()...)
	require.NoError(t, err)
	require.NoError(t, proc.Process())

	db, err := sql.Open("sqlite", out)
	require.NoError(t, err)

	defer func() { require.NoError(t, db.Close()) }()

	var tables []string

	rows, err := db.Query(`SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name`)
	require.NoError(t, err)

	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))

		tables = append(tables, name)
	}

	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())

	assert.Equal(t, []string{"t", "t_absent", "t_columns", "t_duplicate_seq", "t_part2", "t_part3"}, tables)

	// Reassemble wide rows from shards by the source row key.
	reassembled, err := db.Query(`SELECT t1.a, t1.b, t2.c, t2.d, t3.e
		FROM t t1
		JOIN t_part2 t2 ON t2._seq_id = t1._seq_id
		JOIN t_part3 t3 ON t3._seq_id = t1._seq_id
		ORDER BY t1._seq_id`)
	require.NoError(t, err)

	var got [][]any

	for reassembled.Next() {
		var (
			a, e sql.NullInt64
			b, d sql.NullString
			c    sql.NullFloat64
		)

		require.NoError(t, reassembled.Scan(&a, &b, &c, &d, &e))
		got = append(got, []any{a.Int64, b.String, c.Float64, d.String, e.Int64})
	}

	require.NoError(t, reassembled.Err())
	require.NoError(t, reassembled.Close())

	assert.Equal(t, [][]any{
		{int64(1), "x", 1.5, "y", int64(2)},
		{int64(3), "", 0.0, "w", int64(0)},
		{int64(4), "z", 0.0, "", int64(5)},
	}, got)

	// Missing fields of sparse rows are listed, JSON null (c of seq 2) is not.
	var absent []string

	fields := map[int64]string{}
	absentRows, err := db.Query(`SELECT _seq_id, fields FROM t_absent`)
	require.NoError(t, err)

	for absentRows.Next() {
		var (
			seq int64
			f   string
		)

		require.NoError(t, absentRows.Scan(&seq, &f))
		fields[seq] = f
	}

	require.NoError(t, absentRows.Err())
	require.NoError(t, absentRows.Close())

	require.Len(t, fields, 2)

	require.NoError(t, json.Unmarshal([]byte(fields[2]), &absent))
	assert.Equal(t, []string{"b", "e"}, absent)

	require.NoError(t, json.Unmarshal([]byte(fields[3]), &absent))
	assert.Equal(t, []string{"c", "d"}, absent)

	// No duplicate source rows in this input.
	var dupCount int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM t_duplicate_seq`).Scan(&dupCount))
	assert.Equal(t, 0, dupCount)

	// Catalog maps every field to its shard table and column.
	type col struct {
		table, column, source, typ, role string
		ordinal                          int
	}

	catalogRows, err := db.Query(`SELECT table_name, column_name, column_ordinal, source_key, value_type, role
		FROM t_columns ORDER BY table_name, column_ordinal`)
	require.NoError(t, err)

	var catalog []col

	for catalogRows.Next() {
		var c col
		require.NoError(t, catalogRows.Scan(&c.table, &c.column, &c.ordinal, &c.source, &c.typ, &c.role))

		catalog = append(catalog, c)
	}

	require.NoError(t, catalogRows.Err())
	require.NoError(t, catalogRows.Close())

	assert.Equal(t, []col{
		{table: "t", column: "_seq_id", ordinal: 0, source: "_seq_id", typ: "integer", role: "row_key"},
		{table: "t", column: "a", ordinal: 1, source: ".a", typ: "int", role: "data"},
		{table: "t", column: "b", ordinal: 2, source: ".b", typ: "string", role: "data"},
		{table: "t_part2", column: "_seq_id", ordinal: 0, source: "_seq_id", typ: "integer", role: "row_key"},
		{table: "t_part2", column: "c", ordinal: 1, source: ".c", typ: "float", role: "data"},
		{table: "t_part2", column: "d", ordinal: 2, source: ".d", typ: "string", role: "data"},
		{table: "t_part3", column: "_seq_id", ordinal: 0, source: "_seq_id", typ: "integer", role: "row_key"},
		{table: "t_part3", column: "e", ordinal: 1, source: ".e", typ: "int", role: "data"},
	}, catalog)
}
