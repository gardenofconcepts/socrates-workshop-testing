# ScCraTest Workshop - Testing

## Sample Project

This project fetches a list of electric vehicles from an online source and provides functionality to query and filter
them.

### Objectives

* Retrieve the latest electric vehicle data once per day (no live updates).
* Store data in a database to ensure fast access.
* Allow users to:
    * List all available brands.
    * Filter vehicles by brand.
* Fully test the application, including unit tests and integration tests.

### AC

* Daily freshness: The system always serves the most recent data from the current day.
* Database usage: Data is stored locally in a database to improve query speed.
* Filtering: The API allows filtering vehicles by their brand.

## Requirements

* docker
* vscode / pycharm (for `devcontainers`)
* python + uv

## Run

### Tests + Coverage

    uv run pytest -m "not e2e"
    uv run pytest -m e2e
    uv run pytest --cov-report term-missing --cov=app
    uv run pytest --cov-report html --cov=app

## Links

* Python Cheatsheet: https://cheatsheets.zip/python
