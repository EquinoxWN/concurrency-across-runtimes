# OSV-Scanner (https://google.github.io/osv-scanner/) checks every resolved Maven dependency.
OSV ?= osv-scanner

.PHONY: setup lint test scheduling bench audit ci

# PY is the Python to use (the repo's .venv locally, setup-python's in CI); MVNLOCAL (set by the
# author's repoenv helper) keeps the Maven repository inside the repo and is empty in CI.
PY ?= python
MVN = mvn -B -q $(MVNLOCAL)

setup:
	cd python && $(PY) -m pip install --upgrade pip && $(PY) -m pip install -e ".[dev]"
	cd js && npm ci

# javac with every warning as an error; ruff, ruff format and mypy --strict; tsc --checkJs --strict.
lint:
	cd java && $(MVN) -DskipTests compile
	cd python && $(PY) -m ruff check . && $(PY) -m ruff format --check . && $(PY) -m mypy
	cd js && npm run -s lint

# The five problems in every runtime, all reading spec/scenarios.json.
test:
	cd java && $(MVN) verify
	cd python && $(PY) -m pytest -q
	cd js && npm test

# How each runtime schedules 10,000 sleeping tasks and CPU-bound work (Markdown tables).
scheduling:
	cd java && $(MVN) -DskipTests compile && java -cp target/classes portfolio.concurrency.Scheduling
	cd python && $(PY) -m concurrency_across_runtimes.scheduling
	cd js && node src/scheduling.js

bench:
	@echo "M3: throughput and latency per problem as cores increase (JMH, pyperf, mitata)"

# Known vulnerabilities in Python, npm and Maven dependencies.
audit:
	cd python && $(PY) -m pip_audit --skip-editable --cache-dir ../.tmp/pip-audit
	cd js && npm audit --audit-level=high
	cd java && mvn -B -q org.cyclonedx:cyclonedx-maven-plugin:2.9.3:makeAggregateBom -DoutputFormat=json -DoutputName=bom -DincludeTestScope=true
	$(OSV) scan source -L java/target/bom.json

ci: setup lint test
