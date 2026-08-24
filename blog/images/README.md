# Screenshots for the CNPG + Barman + Kasten blog post

The post embeds a Mermaid flow diagram (renders natively on GitHub — no file needed) and
references the screenshots below. Capture these from your own dashboards and save them here
with these exact filenames so the `![...](images/...)` references resolve.

| Filename | Where to capture it | Shows |
|---|---|---|
| `minio-cnpg-barman.png` | MinIO console → Buckets → `cnpg-barman` | `base/` and `wals/` prefixes — proof the physical backup + WAL landed |
| `k10-policies-run.png` | K10 dashboard → Policies | The two policies + a green successful run of `postgres-backup` |
| `k10-restorepoint.png` | K10 → the policy's restore points → expand one | The captured `backups.postgresql.cnpg.io` object alongside the namespace resources |

Optional extras that work well:
- The recovered cluster + your marker rows returning after a restore (terminal or GUI).
- The `WAL ... not found` failure from Gotcha #3 — an honest screenshot of a real failure.

**Redaction:** bucket names, the `192.168.169.x` IPs, and namespaces are fine to show.
Do **not** capture secret values (`pg-app`, `barman`) or a login prompt with a password typed.
