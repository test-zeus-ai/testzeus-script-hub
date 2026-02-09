# DAG Runner for TestZeus

A CLI tool for executing TestZeus tests based on a DAG (Directed Acyclic Graph) structure, handling dependencies and parallel execution with fail-fast logic.

## Prerequisites

- Python 3.8 or higher installed on your machine
- TestZeus account credentials

## Included Files

| File | Description |
|------|-------------|
| `dag_runner.pyz` | The executable (self-contained with all dependencies) |
| `credentials.txt.example` | Template for your credentials |
| `dag_config.json` | DAG configuration file |

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
  }
}
```

**Configuration structure:**

- `test_map`: Maps step names to TestZeus test IDs
- `dag`: Defines dependencies (step -> list of dependencies)
  - `"login": []` - No dependencies, runs first
  - `"checkout": ["login"]` - Runs after "login" completes

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
| `--execution-mode` | `lenient` | Execution mode: `lenient` or `strict` |
| `--poll-interval` | `30` | Seconds between status polls |
| `--timeout` | `3600` | Timeout per node in seconds |

## Examples

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
