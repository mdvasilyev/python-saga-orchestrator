.PHONY: format tests lint

lint: format
test: tests


format:
	@ruff format .
	@ruff check . --fix

tests:
	docker compose up \
		--build \
		--abort-on-container-exit \
		--exit-code-from tests
	docker compose down -v