# Makefile for Data Career Navigator ETL Workflow

update-exchange-rate:
	git pull

# Step 2: Data ingestion
run-data-ingestion:
	python src/data_ingestion.py

# Step 3: Manual step for scraping header text
scrape-header:
	@echo "Please run: python src/scrape_header_text_selenium.py (manual LinkedIn login required)"

# Step 4.1: Local ETL
run-etl-local:
	python src/etl.py

# Step 4.2: Cloud ETL (MotherDuck)
run-etl-motherduck:
	python src/etl.py load_motherduck

# Full workflow (except manual step)
full:
	$(MAKE) update-exchange-rate
	$(MAKE) run-data-ingestion
	$(MAKE) scrape-header
	$(MAKE) run-etl-local
	$(MAKE) run-etl-motherduck

.PHONY: update-exchange-rate run-data-ingestion scrape-header run-etl-local run-etl-motherduck full
