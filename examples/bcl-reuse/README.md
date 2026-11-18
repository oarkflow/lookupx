# Reusable BCL config example

This example demonstrates and checks BCL reuse patterns used by LookupX:

- Bare constant references, such as `dsn MYSQL_DSN`.
- Named block spreads for datasource columns, index defaults, schema fields,
  lookup rules, and bulk settings.
- Local datasource columns and index fields alongside shared blocks.
- Local bulk options alongside a shared batch size.

Run from the repository root:

```sh
MYSQL_DSN='user:pass@tcp(localhost:3306)/lookupx?parseTime=true&loc=UTC' go run ./examples/bcl-reuse
```

To check the full LookupX config with the same assertions:

```sh
MYSQL_DSN='user:pass@tcp(localhost:3306)/lookupx?parseTime=true&loc=UTC' go run ./examples/bcl-reuse config/lookupx.bcl
```

The checker parses BCL and validates resolved values; it does not connect to
MySQL.
