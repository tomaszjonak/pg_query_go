package pg_query_test

import (
	"regexp"
	"strings"
	"testing"

	pg_query "github.com/tomaszjonak/pg_query_go"
	nodes "github.com/tomaszjonak/pg_query_go/nodes"
)

type Query struct {
	Name  string
	Query string
}

var queries = map[string][]Query{
	"SELECT": {
		{
			"basic statement",
			`SELECT "a" AS b FROM "x" WHERE "y" = 5 AND "z" = "y"`,
		},
		{
			"basic statement with schema",
			`SELECT "a" AS b FROM "public"."x" WHERE "y" = 5 AND "z" = "y"`,
		},
		{
			"with DISTINCT",
			`SELECT DISTINCT "a", "b", * FROM "c" WHERE "d" = "e"`,
		},
		{
			"complex SELECT statement",
			`SELECT "memory_total_bytes", "memory_swap_total_bytes" - "memory_swap_free_bytes" AS swap, date_part(?, "s"."collected_at") AS collected_at FROM "snapshots" s JOIN "system_snapshots" ON "snapshot_id" = "s"."id" WHERE "s"."database_id" = ? AND "s"."collected_at" >= ? AND "s"."collected_at" <= ? ORDER BY "collected_at" ASC`,
		},
		{
			"with specific column alias",
			`SELECT * FROM (VALUES ('anne', 'smith'), ('bob', 'jones'), ('joe', 'blow')) names("first", "last")`,
		},
		{
			"with LIKE filter",
			`SELECT * FROM "users" WHERE "name" LIKE 'postgresql:%';`,
		},
		{
			"with NOT LIKE filter",
			`SELECT * FROM "users" WHERE "name" NOT LIKE 'postgresql:%';`,
		},
		{
			"simple WITH statement",
			`WITH t AS (SELECT random() AS x FROM generate_series(1, 3)) SELECT * FROM "t"`,
		},
		{
			"complex WITH statement",
			// Taken from http://www.postgresql.org/docs/9.1/static/queries-with.html
			`
			WITH RECURSIVE search_graph ("id", "link", "data", "depth", "path", "cycle") AS (
		        SELECT "g"."id", "g"."link", "g"."data", 1,
		          ARRAY[ROW("g"."f1", "g"."f2")],
		          false
		        FROM "graph" g
		      UNION ALL
		        SELECT "g"."id", "g"."link", "g"."data", "sg"."depth" + 1,
		          "path" || ROW("g"."f1", "g"."f2"),
		          ROW("g"."f1", "g"."f2") = ANY("path")
		        FROM "graph" g, "search_graph" sg
		        WHERE "g"."id" = "sg"."link" AND NOT "cycle"
		    )
		    SELECT "id", "data", "link" FROM "search_graph";
		    `,
		},
		{
			"SUM",
			`SELECT sum("price_cents") FROM "products"`,
		},
		{
			"LATERAL",
			`SELECT "m"."name" AS mname, "pname" FROM "manufacturers" m, LATERAL get_product_names("m"."id") pname`,
		},
		{
			"LATERAL JOIN",
			`
			SELECT "m"."name" AS mname, "pname"
		      FROM "manufacturers" m LEFT JOIN LATERAL get_product_names("m"."id") pname ON true
		    `,
		},
		{
			"CROSS JOIN",
			`SELECT "x", "y" FROM "a" CROSS JOIN "b"`,
		},

		{
			"NATURAL JOIN",
			`SELECT "x", "y" FROM "a" NATURAL JOIN "b"`,
		},

		{
			"LEFT JOIN",
			`SELECT "x", "y" FROM "a" LEFT JOIN "b" ON 1 > 0`,
		},

		{
			"RIGHT JOIN",
			`SELECT "x", "y" FROM "a" RIGHT JOIN "b" ON 1 > 0`,
		},

		{
			"FULL JOIN",
			`SELECT "x", "y" FROM "a" FULL JOIN "b" ON 1 > 0`,
		},

		{
			"JOIN with USING",
			`SELECT "x", "y" FROM "a" JOIN "b" USING ("z")`,
		},
		{
			"omitted FROM clause",
			`SELECT 2 + 2`,
		},
		{
			"IS NULL",
			`SELECT * FROM "x" WHERE "y" IS NULL`,
		},
		{
			"IS NOT NULL",
			`SELECT * FROM "x" WHERE "y" IS NOT NULL`,
		},
		{
			"COUNT",
			`SELECT count(*) FROM "x" WHERE "y" IS NOT NULL`,
		},
		{
			"COUNT DISTINCT",
			`SELECT count(DISTINCT "a") FROM "x" WHERE "y" IS NOT NULL`,
		},
		{
			"basic CASE WHEN statements",
			`SELECT CASE WHEN "a"."status" = 1 THEN 'active' WHEN "a"."status" = 2 THEN 'inactive' END FROM "accounts" a`,
		},
		{
			"CASE condition WHEN clause",
			`SELECT CASE 1 > 0 WHEN true THEN 'ok' ELSE NULL END`,
		},
		{
			"CASE WHEN statements with ELSE clause",
			`SELECT CASE WHEN "a"."status" = 1 THEN 'active' WHEN "a"."status" = 2 THEN 'inactive' ELSE 'unknown' END FROM "accounts" a`,
		},
		{
			"CASE WHEN statements in WHERE clause",
			`SELECT * FROM "accounts" WHERE "status" = CASE WHEN "x" = 1 THEN 'active' ELSE 'inactive' END`,
		},
		{
			"CASE WHEN EXISTS",
			`SELECT CASE WHEN EXISTS(SELECT 1) THEN 1 ELSE 2 END`,
		},
		{
			"Subselect in SELECT clause",
			`SELECT (SELECT 'x')`,
		},
		{
			"Subselect in FROM clause",
			`SELECT * FROM (SELECT generate_series(0, 100)) a`,
		},
		{
			"IN expression",
			`SELECT * FROM "x" WHERE "id" IN (1, 2, 3)`,
		},
		{
			"IN expression Subselect",
			`SELECT * FROM "x" WHERE "id" IN (SELECT "id" FROM "account")`,
		},
		{
			"NOT IN expression",
			`SELECT * FROM "x" WHERE "id" NOT IN (1, 2, 3)`,
		},
		{
			"Subselect JOIN",
			`SELECT * FROM "x" JOIN (SELECT "n" FROM "z") b ON "a"."id" = "b"."id"`,
		},
		{
			"simple indirection",
			`SELECT * FROM "x" WHERE "y" = "z"[?]`,
		},
		{
			"complex indirection",
			`SELECT * FROM "x" WHERE "y" = "z"[?][?]`,
		},
		{
			"NOT",
			`SELECT * FROM "x" WHERE NOT "y"`,
		},
		{
			"OR",
			`SELECT * FROM "x" WHERE "x" OR "y"`,
		},
		{
			"OR with parens",
			`SELECT 1 WHERE (1 = 1 OR 1 = 2) AND 1 = 2`,
		},
		{
			"OR with nested AND",
			`SELECT 1 WHERE (1 = 1 AND 2 = 2) OR 2 = 3`,
		},
		{
			"OR with nested OR",
			`SELECT 1 WHERE 1 = 1 OR 2 = 2 OR 2 = 3`,
		},
		{
			"ANY",
			`SELECT * FROM "x" WHERE "x" = ANY(?)`,
		},
		{
			"COALESCE",
			`SELECT * FROM "x" WHERE "x" = COALESCE("y", ?)`,
		},
		{
			"GROUP BY",
			`SELECT "a", "b", max("c") FROM "c" WHERE "d" = 1 GROUP BY "a", "b"`,
		},
		{
			"LIMIT",
			`SELECT * FROM "x" LIMIT 50`,
		},
		{
			"OFFSET",
			`SELECT * FROM "x" OFFSET 50`,
		},
		{
			"FLOAT",
			`SELECT "amount" * 0.5`,
		},
		{
			"BETWEEN",
			`SELECT * FROM "x" WHERE "x" BETWEEN '2016-01-01' AND '2016-02-02'`,
		},
		{
			"NOT BETWEEN",
			`SELECT * FROM "x" WHERE "x" NOT BETWEEN '2016-01-01' AND '2016-02-02'`,
		},
		{
			"BETWEEN SYMMETRIC",
			`SELECT * FROM "x" WHERE "x" BETWEEN SYMMETRIC 20 AND 10`,
		},
		{
			"NOT BETWEEN SYMMETRIC",
			`SELECT * FROM "x" WHERE "x" NOT BETWEEN SYMMETRIC 20 AND 10`,
		},
		{
			"NULLIF",
			`SELECT NULLIF("id", 0) AS id FROM "x"`,
		},
		{
			"return NULL",
			`SELECT NULL FROM "x"`,
		},
		{
			"IS true",
			`SELECT * FROM "x" WHERE "y" IS TRUE`,
		},
		{
			"IS NOT true",
			`SELECT * FROM "x" WHERE "y" IS NOT TRUE`,
		},
		{
			"IS false",
			`SELECT * FROM "x" WHERE "y" IS FALSE`,
		},
		{
			"IS NOT false",
			`SELECT * FROM "x" WHERE "y" IS NOT FALSE`,
		},
		{
			"IS unknown",
			`SELECT * FROM "x" WHERE "y" IS UNKNOWN`,
		},
		{
			"IS NOT unknown",
			`SELECT * FROM "x" WHERE "y" IS NOT UNKNOWN`,
		},
		{
			"with columndef list",
			`
			SELECT * FROM crosstab(
		    'SELECT "department", "role", COUNT("id") FROM "users" GROUP BY "department", "role" ORDER BY "department", "role"',
		    'VALUES (''admin''::text), (''ordinary''::text)')
		    AS (department varchar, admin int, ordinary int)
		    `,
		},

		{
			"with columndef list and alias",
			`
			SELECT * FROM crosstab(
		    'SELECT "department", "role", COUNT("id") FROM "users" GROUP BY "department", "role" ORDER BY "department", "role"',
		    'VALUES (''admin''::text), (''ordinary''::text)')
		    ctab (department varchar, admin int, ordinary int)
		    `,
		},
		{
			"with columndef list returning an array",
			`
			SELECT "row_cols"[0] AS dept, "row_cols"[1] AS sub, "admin", "ordinary" FROM crosstab(
		    'SELECT ARRAY["department", "sub"] AS row_cols, "role", COUNT("id") FROM "users" GROUP BY "department", "role" ORDER BY "department", "role"',
		    'VALUES (''admin''::text), (''ordinary''::text)')
		    AS (row_cols varchar[], admin int, ordinary int)
		    `,
		},
		{
			"with window function",
			`WITH cte_raw_data AS (SELECT i_start_time, i_device_id, input_port, row_number() OVER (PARTITION BY i_device_id, input_port ORDER BY i_start_time ASC) FROM foo WHERE i_start_time >= '2020-09-28 10:19:38' AND i_start_time < '2020-09-29 10:19:38' GROUP BY i_start_time, i_device_id, input_port) SELECT 1`,
		},
	},
}

func TestDeparse(t *testing.T) {
	for category, queries := range queries {
		t.Run(category, func(t *testing.T) {
			for _, query := range queries {
				t.Run(query.Name, func(t *testing.T) {
					tree, err := pg_query.Parse(query.Query)
					if err != nil {
						t.Errorf("Parse error %s", err)
					}
					deparsed, err := pg_query.Deparse(tree)
					if err != nil {
						t.Errorf("Deparse error %s", err)
					}
					// let(:oneline_query) { query.gsub(/\s+/, ' ').gsub('( ', '(').gsub(' )', ')').strip.chomp(';') }
					onelineQuery := regexp.MustCompile(`\s+`).ReplaceAllString(query.Query, " ")
					onelineQuery = strings.Replace(onelineQuery, "( ", "(", -1)
					onelineQuery = strings.Replace(onelineQuery, " )", ")", -1)
					onelineQuery = strings.TrimSpace(onelineQuery)
					onelineQuery = strings.TrimSuffix(onelineQuery, ";")
					if onelineQuery != deparsed {
						t.Errorf("mismatch\n%s\n%s", query.Query, deparsed)
					}
				})
			}
		})
	}
}

func TestFoo2(t *testing.T) {
	var n nodes.Node = nodes.BoolExpr{
		Boolop: nodes.AND_EXPR,
		Args: nodes.List{Items: []nodes.Node{
			&nodes.A_Expr{
				Name:  nodes.List{Items: []nodes.Node{nodes.String{Str: "="}}},
				Kind:  nodes.AEXPR_OP,
				Lexpr: nodes.ColumnRef{Fields: nodes.List{Items: []nodes.Node{nodes.String{Str: "i_debug_info"}}}},
				Rexpr: nodes.A_Const{Val: nodes.String{Str: "query.nonflow=t,query.cache.skip=t"}},
			},
		}},
	}

	s, err := pg_query.DeparseItem(n)
	if err != nil {
		t.Fatal(err)
	}

	t.Log(s)
	t.FailNow()
}
