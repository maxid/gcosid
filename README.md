# gcosid

Segment and snowflake ID generation in Go, compatible with the CosID concepts used by XDMP.

> Status: **experimental scaffold** (v0.0.0-dev).

## Planned features

- Snowflake IDs with configurable machine bits
- Segment (DB-backed) allocator with prefetch
- Chain mode and ID prefix conversion
- Same numeric layout/constraints as existing IDs

## Install

```bash
go get github.com/maxid/gcosid
```

## License

MIT. See [LICENSE](LICENSE).
