.PHONY: install install-dev test test-cov clean lint format

install:
	pip install -e .

install-dev:
	pip install -e ".[dev]"

test:
	pytest -q

test-cov:
	pytest --cov=epanet_utils --cov-report=html --cov-report=term

clean:
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info/
	rm -rf .pytest_cache/
	rm -rf htmlcov/
	rm -rf .coverage
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

lint:
	pylint src/epanet_utils/

format:
	black src/epanet_utils/ tests/
