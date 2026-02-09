# TestZeus Script Hub

A curated collection of ready-to-use scripts and automations built with the [testzeus-sdk](https://pypi.org/project/testzeus-sdk/). Each script solves a real-world test orchestration problem — grab one, configure it, and run.

## What's Inside

| Script | Description | Docs |
|--------|-------------|------|
| [dag-runner](dag-runner/) | Execute tests as a DAG — define dependencies, run in parallel, fail-fast on errors | [README](dag-runner/README.md) |

> More scripts coming soon. Have an idea? [Open an issue.](#contributing)

## Quick Start

### 1. Get your TestZeus credentials

Sign up at [testzeus.com](https://testzeus.com) and grab your email/password.

### 2. Pick a script

Each script lives in its own directory with a self-contained README, example configs, and a ready-to-run `.pyz` executable (no pip install needed — just Python 3.8+).

### 3. Run it

```bash
cd dag-runner
cp credentials.txt.example credentials.txt
# Edit credentials.txt with your email/password

python dag_runner.pyz --config dag_config.json
```

That's it. Results are written to a JSON file, and exit codes are CI-friendly (0 = pass, 1 = partial failure, 2 = failure).

## Repository Structure

```
testzeus-script-hub/
├── README.md                    # You are here
├── .github/workflows/           # CI — auto-builds .pyz on push
├── dag-runner/
│   ├── README.md                # Full docs for the DAG runner
│   ├── dag_runner.py            # Source code
│   ├── pyproject.toml           # Package metadata & dependencies
│   ├── dag_runner.pyz           # Auto-built executable (committed by CI)
│   ├── dag_config.json          # Example DAG configuration
│   └── credentials.txt.example
└── ...                          # Future scripts
```

Each script directory follows the same pattern:

- **`*.py` + `pyproject.toml`** — Source code and dependencies
- **`*.pyz`** — Self-contained executable (built with [shiv](https://github.com/linkedin/shiv), auto-rebuilt by CI)
- **`README.md`** — Setup, usage, and configuration docs
- **Config/example files** — Ready to copy and customize

## About TestZeus

[TestZeus](https://testzeus.com) is an AI-powered test automation platform. Write test cases in plain English or Gherkin, and autonomous agents handle the execution across browsers and environments.

The [`testzeus-sdk`](https://pypi.org/project/testzeus-sdk/) is the Python SDK for programmatic access to the platform — create test runs, poll for results, manage groups, and build custom orchestration on top.

## Contributing

Contributions are welcome! If you've built something useful with `testzeus-sdk`, consider adding it here.

### Adding a new script

1. Create a new directory with a descriptive name (e.g., `parallel-runner/`, `slack-notifier/`)
2. Include source code (`*.py`) and a `pyproject.toml` with dependencies
3. Include a `README.md` with setup and usage instructions
4. Add example config files with `.example` extensions
5. Add a CI workflow in `.github/workflows/` to auto-build the `.pyz` using [shiv](https://github.com/linkedin/shiv)
6. Update the table in this README
7. Open a pull request

### Guidelines

- Scripts should be self-contained and runnable with just Python 3.8+
- Support credentials via file, environment variables, and CLI arguments
- Use meaningful exit codes for CI/CD integration
- Include example configurations

## License

See [LICENSE](LICENSE) for details.
