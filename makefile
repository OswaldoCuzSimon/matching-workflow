.PHONY: install clean run

clean:
	rm -f gtin_match_products_df.csv product.csv unmatched_products_clean_file.csv

install:
	python -c 'import nltk; nltk.download("punkt")'

run:
	python workflow.py MatchingText --local-scheduler

