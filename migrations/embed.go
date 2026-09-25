// Package migrations embeds the SQL schema files so they ship inside the gateway
// binary. That means `cmd/gateway migrate` works from anywhere without needing
// the source tree present — the migrations travel with the binary.
package migrations

import "embed"

// FS holds every .sql migration, applied in lexical filename order.
//
//go:embed *.sql
var FS embed.FS
