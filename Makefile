clean:
	rm -rf build dist

wheel:
	python3 -m build --wheel

upload:
	twine upload dist/*

build: clean wheel