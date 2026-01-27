# Process Start Methods (Linux)

Python supports three process start methods on Linux:

| Method       | Speed | Safety | Notes                      |
|--------------|-------|--------|----------------------------|
| fork         | Fast  | Unsafe | Copies parent memory state |
| spawn        | Slow  | Safe   | Fresh interpreter          |
| forkserver   | Medium| Safer  | Hybrid approach            |

## fork (default on Linux)

- Child inherits **entire parent state**
- Extremely fast startup
- Dangerous if:
  - Parent has threads
  - Locks are held
  - Native libraries are initialized (BLAS, CUDA, OpenMP)

**Failure mode** :

- Deadlocks
- Hangs
- Corrupted internal states

### Evidence

See:

- `labs/multiprocessing/03_start_method_fork_vs_spawn_vs_forkserver.py`

You observed:

- fork starts fastest
- fork + threads can block on inherited locks

## spawn

- Starts a fresh Python interpreter
- Everything must be **picklable**
- Much slower startup

### When to use

- Safety > performance
- Long-lived workers
- Complex runtimes

## forkserver

- A middle ground
- Avoids forking a threaded parent
- Lower risk than fork
- Faster than spawn

## Production rule

- **Short-lived batch jobs** → fork (carefully)
- **Long-lived services** → forkserver or spawn
- **Threaded parents** → never fork blindly
