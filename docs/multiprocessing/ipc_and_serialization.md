# IPC and Serialization: The Hidden Tax

Multiprocessing performance is often limited by **what you send between processes**, not how many processes you run.

## The rule

> Sending large objects across processes is expensive.

This includes:

- Strings
- Lists
- NumPy arrays
- Python objects

## Lab evidence

From:

- `labs/multiprocessing/02_pickle_ipc_tax_paths_vs_payloads.py`

### NLP example

- Sending file paths: ~840 items/s
- Sending ~8MB text blobs: ~32 items/s

**Same workers. Same CPU.**
Only IPC changed.

## Why this happens

- Python uses **pickle**
- Pickle = serialize → copy → deserialize
- Copies scale with payload size
- Pipes become the bottleneck

## Recommended patterns

### Good

- Send **paths**
- Send **IDs**
- Send **offsets**
- Let workers load data locally

### Bad

- Send raw documents
- Send large arrays
- Send model outputs blindly

## Key takeaway

> IPC cost grows faster than CPU cost.

Always ask:

- “Can the worker load this itself?”
- “Can I send metadata instead?”
