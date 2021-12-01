.PHONY: lint  # Package wheel
lint:
	docker-compose run zegami_sdk flake8 .

.PHONY: package  # Package into wheel and tar
package:
	docker-compose run zegami_sdk bash -c "rm -rf dist/ \
		&& python3 setup.py sdist bdist_wheel \
		&& rm -rf build/"

.PHONY: build
build:
	docker-compose build

.PHONY: shell
shell:
	docker-compose run zegami_sdk

.PHONY: test
test:
	docker-compose run zegami_sdk bash -c \
		"coverage run -m unittest discover && \
		coverage report -m"
