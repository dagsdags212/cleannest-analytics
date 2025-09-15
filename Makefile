DB := cleannest/db/main.db

dashboard:
	uv run streamlit run dashboard.py

sync-db:
	rm -f ${DB}
	uv run cleannest/ingestion.py

.PHONY: sync-db
