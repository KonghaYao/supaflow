# Upstream reference sources

Retrieved on 2026-09-03 from GitHub's official `codeload.github.com` endpoint after direct `github.com` Git transport was unavailable.

| Project | Source snapshot | Archive SHA-256 | License | Usage policy |
| --- | --- | --- | --- | --- |
| FlowGram.AI | `bytedance/flowgram.ai`, `main` | `d514bd9a02148418349cd90df34e583b261cf9f1f9b7fe03b647c1c191c53cb5` | MIT, Copyright 2025 Bytedance Ltd. and/or its affiliates | Editor/runtime model boundaries and node extension concepts only |
| Coze Studio | `coze-dev/coze-studio`, `main` | `63539343e5a62066ac316679fab9650493d5f485671ecf5f133e134c941df7c7` | Apache-2.0 | Product layering, workflow domain seams, and interrupt interaction concepts only |

Local, ignored reference paths:

- `tmp/supaflow-references/flowgram-source`
- `tmp/supaflow-references/coze-source`

Supaflow's durable orchestration, tenancy, storage transactions, HTTP protocol, and operational model are original implementations driven by `docs/design/unified-workflow-platform.md` in the host worktree. No source file has been copied from either upstream snapshot. If future work copies or adapts upstream code, add a row-level provenance entry with the upstream path, revision, license notice, local destination, and modifications before merging it.
