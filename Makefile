# Makefile

test:
	./gradlew test

lib-deploy:
	mvn -f kafka-support/pom.xml deploy

.PHONY: test lib-deploy