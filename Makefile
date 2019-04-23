.PHONY: clean build

clean:
	find commons/ consumer/ consumer_tests/ publisher/ publisher_tests/ -name "*.pyc" -delete

build:
	python setup.py install
	python -m unittest discover
	rm -rf target
	mkdir target
	cp consumer/main.py target
	zip -x publisher/main.py -r target/consumer.zip consumer commons
