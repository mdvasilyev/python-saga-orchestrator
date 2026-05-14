.PHONY: format tests lint

lint: format
test: tests


format:
	@isort .
	@black .

tests:
	@docker compose up --build --abort-on-container-exit
