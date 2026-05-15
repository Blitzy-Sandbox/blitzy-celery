# Celery

Celery is an asynchronous task queue/job queue based on distributed message passing. This repository is the Blitzy fork.

## Why this site is a stub

Celery's upstream documentation lives under `docs/` as **Sphinx reStructuredText (`.rst`)** with a full `conf.py`. TechDocs/MkDocs only renders Markdown, so the existing `.rst` content cannot be surfaced here without a conversion step.

## Where to read the real docs

- **Upstream rendered docs**: <https://docs.celeryq.dev/>
- **Source docs (`.rst` tree)**: <https://github.com/Blitzy-Sandbox/blitzy-celery/tree/main/docs>
- **README**: `README.rst` at the repository root

## To make this site useful

Pick one:

1. Run a `.rst` → Markdown conversion (e.g. `rst2myst` or `pandoc`) over `docs/` and check the output into a sibling `docs-md/` directory; point `docs_dir` at it.
2. Curate a small set of hand-authored Markdown pages covering the highest-value upstream content (getting started, broker config, task patterns) and ship those instead.

## License

Celery is distributed under the BSD 3-Clause License. See the upstream `LICENSE`.
