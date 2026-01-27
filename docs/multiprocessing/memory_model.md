# Multiprocessing Memory Model

Processes do **not** share memory by default.

Every object you send:

- Is copied
- Uses RAM
- Uses CPU to serialize

## Copy-on-write myth

On Linux with fork:

- Memory pages are shared **until modified**
- The moment a page is written → copy happens
- NumPy arrays, buffers, and tensors break this quickly

## Lab evidence

From:

- `labs/multiprocessing/04_shared_memory_doc_images.py`

### Arrays via pickle

- Slower
- Higher memory usage
- More CPU overhead

### Shared memory

- Faster
- Stable memory
- Only metadata crosses IPC boundary

## When to use shared memory

- Images
- Embeddings
- Feature matrices
- Large tensors

## When NOT to

- Small objects
- Short-lived scripts
- Complex lifecycles

## Rule of thumb

> If payload > a few MB → consider shared memory
