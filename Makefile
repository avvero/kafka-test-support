# Makefile

# Variables for Docker image names
IMAGE_NAME := emk
NATIVE_IMAGE_NAME := emk-native
DOCKER_REPO := avvero
VERSION := $(shell grep '^version=' gradle.properties | cut -d '=' -f2)

test:
	./gradlew test

lib-deploy:
	mvn -f kafka-support/pom.xml deploy

.PHONY: test lib-deploy