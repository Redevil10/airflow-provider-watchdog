# Contributing to airflow-plugin-watchdog

Thanks for your interest in improving Watchdog! Bug reports, feature ideas, and
pull requests are all welcome.

## Ways to contribute

- **Report a bug** — open an issue with the Airflow version, the metadata-DB
  backend (PostgreSQL / SQLite / MySQL), and the steps or config that trigger it.
- **Suggest a feature** — open an issue describing the use case. New detectors and
  alert channels are good candidates; please keep the project's
  **zero-dependency, runs-in-the-API-server** design in mind (see
  [Why Watchdog?](README.md#why-watchdog)).
- **Send a pull request** — for anything non-trivial, open an issue first so we can
  agree on the approach before you write code.

## Development setup

Watchdog uses [uv](https://docs.astral.sh/uv/) for environment and dependency
management.

```bash
git clone https://github.com/Redevil10/airflow-plugin-watchdog.git
cd airflow-plugin-watchdog
uv sync --extra dev
```

## Running the tests

The unit suite mocks Airflow and is fast; the integration suite runs the real
detector / dashboard SQL against an actual Airflow metadata database.

```bash
uv run pytest tests/unit          # fast — Airflow mocked
```

For the integration suite (PostgreSQL is the production backend; SQLite is also
exercised), see [Integration tests](README.md#integration-tests) in the README.

Please add or update tests for any behaviour you change — both `tests/unit` and,
where it touches SQL or the results round-trip, `tests/integration`.

## Linting and formatting

Formatting and lint are enforced with [prek](https://github.com/j178/prek)
(pre-commit-compatible) using the hooks in [`prek.toml`](prek.toml) — Ruff for
lint and formatting, plus the standard file-hygiene hooks.

```bash
prek run --all-files            # or: pre-commit run --all-files
```

CI runs the same checks, so it's worth running them locally before you push.

## Pull request checklist

- Branch off `main` and keep the PR focused on a single change.
- `uv run pytest tests/unit` passes (and the integration suite if you touched SQL).
- Lint/format hooks pass.
- Add a note under the top of [`CHANGELOG.md`](CHANGELOG.md) describing the change
  (the maintainer assigns the release version on the version bump).
- Describe the motivation and the behaviour change in the PR body.

## Reporting security issues

Please **do not** open a public issue for a security vulnerability. Instead,
report it privately to the maintainer so it can be addressed before disclosure.

## License

By contributing, you agree that your contributions are licensed under the
project's [Apache License 2.0](LICENSE).
