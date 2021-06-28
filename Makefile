.PHONY: package  # Package wheel
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
