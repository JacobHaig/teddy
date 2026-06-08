# Phase 7 — JSON Reader Fixes Implementation Plan

> Roadmap Phase 7. Most of the original scope was absorbed by Phase 12 B2
> (mixed-type coercion, @trunc fix, inference cleanup). Remaining = the
> `.auto => unreachable` headline + a few real reader gaps. One commit.
> Commit policy: commit on green (no review gate this phase — user reviews
> at Phase 8).

**Baseline:** 683/683 tests at b8c73cb (pushed).

## Fixes (src/dataframe/json_reader.zig + tests)

1. **`.auto => unreachable` (line 31).** `parse` resolves the format then
   switches with an `unreachable` auto arm — a latent panic if the resolution
   ever returns `.auto`. Fix by construction: `detectFormat` returns a new
   `DetectedFormat = enum { rows, columns, ndjson }` (no auto member), and the
   dispatch switch becomes exhaustive over THAT enum — no `unreachable`. The
   public `JsonFormat` (with `.auto`) stays the option type; map it to
   `DetectedFormat` once (explicit `.auto => detectFormat(...)`, others 1:1).

2. **`detectFormat` fragility (review IO-L2).** The `\n{` substring heuristic
   misdetects a pretty-printed columns/rows doc that happens to contain a
   newline-then-brace. Replace with: leading `[` → rows; leading `{` → ndjson
   iff there are ≥2 non-empty lines that each (trimmed) start with `{`,
   else columns. Document the heuristic + that `.format` can force it.

3. **Object keys aren't unescaped.** Keys use `parseString` (raw escaped
   slice) while values use `parseStringAlloc`. `{"a\"b": 1}` yields a wrong
   column name. Route keys through the unescaping path; since unescaped keys
   are owned, store them owned and free in the existing col_names defer
   (col_names becomes `ArrayList([]u8)` / freed). Verify both parseRows/
   parseColumns/parseNdjson and the dedup `findColIndex` still work
   (comparison is by bytes — fine).

4. **Integer overflow → float fallback.** `parseNumber` does
   `parseInt(i64) catch return error.InvalidJson` — a valid JSON integer
   beyond i64 range wrongly errors. On overflow, fall back to parsing the
   same token as f64 (JSON numbers are conceptually arbitrary-precision;
   f64 is the lossy-but-valid landing spot, consistent with how other readers
   treat big numerics). Only InvalidCharacter-class failures error.

5. **String escape completeness.** `parseStringAlloc` handles
   `" \ n r t /` but drops `\b`/`\f` into the raw `else`. Add `b`→0x08,
   `f`→0x0C. `\uXXXX` stays a documented gap (needs code-point→UTF-8 +
   surrogate handling — out of scope; note it in a comment + the roadmap).

## Tests
- `.auto` detect: `[...]`→rows, `{"c":[...]}`→columns, `{...}\n{...}`→ndjson;
  and the fragility case: a single columns object pretty-printed with
  newlines+braces inside string values still detects columns.
- escaped key round-trips to the right column name (`{"a\"b": 1}` →
  column named `a"b`).
- a 20-digit integer parses (as float, value approx-correct) instead of
  erroring.
- `\b`/`\f` in a string value unescape.
- full existing JSON suite green (the absorbed Phase-12 behavior unchanged).

## Out of scope
Reading nested JSON (arrays/objects as row values) into `Nested` — large,
own feature; the reader still errors on a `[`/`{` value token. `\uXXXX`
unicode escapes. Writer is complete for the current type set (no changes).
