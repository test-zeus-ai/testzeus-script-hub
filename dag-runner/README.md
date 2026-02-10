# DAG Runner for TestZeus

A CLI tool for executing TestZeus tests based on a DAG (Directed Acyclic Graph) structure, handling dependencies and parallel execution with fail-fast logic.

## Prerequisites

- Python 3.11 or higher installed on your machine
- TestZeus account credentials

## Included Files

| File | Description |
|------|-------------|
| `dag_runner.pyz` | The executable (auto-built from source, self-contained with all dependencies) |
| `dag_runner.py` | Source code |
| `pyproject.toml` | Package metadata and dependencies |
| `credentials.txt.example` | Template for your credentials |
| `dag_config.json` | DAG configuration file |

> The `.pyz` is automatically rebuilt by CI whenever `dag_runner.py` or `pyproject.toml` is updated on `main`.

## Setup

### 1. Configure credentials

There are three ways to provide credentials (in order of precedence):

**Option A: Credentials file (recommended)**

Copy the example file and add your TestZeus credentials:

**Windows:**
```cmd
copy credentials.txt.example credentials.txt
```

**macOS/Linux:**
```bash
cp credentials.txt.example credentials.txt
```

Edit `credentials.txt` with your credentials:

```
TESTZEUS_EMAIL=your-email@example.com
TESTZEUS_PASSWORD=your-password-here
```

> **Note:** If a `credentials.txt` file exists in the current directory, it is loaded automatically — you don't need to pass `--credentials-file` explicitly.

**Option B: Environment variables**

```bash
export TESTZEUS_EMAIL="your-email@example.com"
export TESTZEUS_PASSWORD="your-password-here"
```

**Option C: CLI arguments**

```bash
./dag_runner.pyz --config dag_config.json --email user@example.com --password secret
```

### 2. Configure the DAG (optional)

Edit `dag_config.json` to define your test execution flow:

```json
{
  "test_map": {
    "step_name": "testzeus_test_id",
    "login": "abc123xyz",
    "checkout": "def456uvw"
  },
  "dag": {
    "login": [],
    "checkout": ["login"]
  },
  "test_env": "your-environment-id",
  "notification_channels": ["channel-id-1", "channel-id-2"]
}
```

**Configuration structure:**

| Field | Required | Description |
|-------|----------|-------------|
| `test_map` | Yes | Maps step names to TestZeus test IDs |
| `dag` | Yes | Defines dependencies (step -> list of dependencies) |
| `test_env` | No | Test environment ID to assign to each test run group |
| `notification_channels` | No | List of notification channel IDs |

- `dag` values: `"login": []` = no dependencies (runs first), `"checkout": ["login"]` = runs after "login" completes
- `test_env` and `notification_channels` can also be provided via CLI args. CLI args take precedence over config JSON values.

## Usage

### Basic usage (auto-loads `credentials.txt` from current directory)

**Windows:**
```cmd
python dag_runner.pyz --config dag_config.json
```

**macOS/Linux:**
```bash
./dag_runner.pyz --config dag_config.json
```

### With explicit credentials file

```bash
python dag_runner.pyz --config dag_config.json --credentials-file /path/to/credentials.txt
```

### With environment variables

```bash
export TESTZEUS_EMAIL="user@example.com"
export TESTZEUS_PASSWORD="secret"
python dag_runner.pyz --config dag_config.json
```

### With CLI arguments

```bash
python dag_runner.pyz --config dag_config.json --email user@example.com --password secret
```

### With custom output file

```bash
python dag_runner.pyz --config dag_config.json --output results.json
```

### All options

```bash
./dag_runner.pyz --help
```

| Option | Default | Description |
|--------|---------|-------------|
| `-c, --config` | (required) | Path to JSON configuration file |
| `-o, --output` | `dag_results.json` | Output JSON file path |
| `--credentials-file` | - | Path to credentials file (e.g. `credentials.txt`) |
| `--email` | - | TestZeus email (alternative to env file) |
| `--password` | - | TestZeus password (alternative to env file) |
| `--base-url` | `https://pb.prod.testzeus.app` | TestZeus API base URL |
| `--poll-interval` | `30` | Seconds between status polls |
| `--timeout` | `3600` | Timeout per node in seconds |
| `--test-env` | - | Test environment ID to assign to each test run group |
| `--notification-channels` | - | Space-separated list of notification channel IDs |

## Examples

### With test environment and notifications

```bash
python dag_runner.pyz --config dag_config.json --credentials-file credentials.txt \
  --test-env abc123def456ghi \
  --notification-channels ch1abc123def456 ch2xyz789abc123
```

### Sequential execution (A -> B -> C)

```json
{
  "test_map": {
    "step_a": "test_id_1",
    "step_b": "test_id_2",
    "step_c": "test_id_3"
  },
  "dag": {
    "step_a": [],
    "step_b": ["step_a"],
    "step_c": ["step_b"]
  }
}
```

### Parallel execution (A and B run together, then C)

```json
{
  "test_map": {
    "step_a": "test_id_1",
    "step_b": "test_id_2",
    "step_c": "test_id_3"
  },
  "dag": {
    "step_a": [],
    "step_b": [],
    "step_c": ["step_a", "step_b"]
  }
}
```

### Diamond pattern

```
    A
   / \
  B   C
   \ /
    D
```

```json
{
  "test_map": {
    "A": "test_id_a",
    "B": "test_id_b",
    "C": "test_id_c",
    "D": "test_id_d"
  },
  "dag": {
    "A": [],
    "B": ["A"],
    "C": ["A"],
    "D": ["B", "C"]
  }
}
```

## Output

Results are written to a JSON file (default: `dag_results.json`) containing:

- Overall execution status
- Duration and timing information
- Per-node status and error details
- Execution order by level

### Exit codes

| Code | Status |
|------|--------|
| 0 | All tests passed |
| 1 | Partial failure (some tests passed) |
| 2 | Complete failure |

## Troubleshooting

### "Permission denied" when running (macOS/Linux)

```bash
chmod +x dag_runner.pyz
```

### Module not found errors

Clear the shiv cache:

**Windows:**
```cmd
rmdir /s /q %USERPROFILE%\.shiv
```

**macOS/Linux:**
```bash
rm -rf ~/.shiv/dag_runner.pyz_*
```

### Specify Python version

If you have multiple Python versions, run with a specific one:

```cmd
python3.12 dag_runner.pyz --config dag_config.json --credentials-file credentials.txt
```

### Credentials not loading

Ensure you're using `--credentials-file` explicitly:

```cmd
python dag_runner.pyz --config dag_config.json --credentials-file credentials.txt
```

## Fail-Fast Behavior

If a test fails, all dependent tests are automatically skipped. Independent branches continue to execute.

Example: If B depends on A, and A fails:
- A: FAILED
- B: SKIPPED (dependency failed)
- C (independent): Continues to run

## Development

The `.pyz` is built with [shiv](https://github.com/linkedin/shiv) and automatically rebuilt by GitHub Actions on every push to `main` that touches `dag_runner.py` or `pyproject.toml`.

### Building locally

```bash
pip install shiv
cd dag-runner
shiv -c dag_runner -o dag_runner.pyz --compressed .
```
