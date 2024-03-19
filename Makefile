############################## Makefile repo variables ##############################
DOCKER_IMAGE_NAME?=cricketdataservice
BUILD_ENV := $(shell echo "$$BUILD_ENV")
DOCKER_IMAGE_VERSION?=latest
DOCKER_IMAGE_TAG=$(BUILD_ENV)-$(DOCKER_IMAGE_VERSION)
DOCKER_REPO_PATH := $(shell echo "$$DOCKER_REPO/")
DOCKER_USERNAME_PATH := $(shell echo "$$DOCKER_USERNAME/$(DOCKER_PROJECT_PATH)")
DOCKER_BASE_IMAGE_NAME := python:3.9
DOCKER_BASE_IMAGE := $(DOCKER_REPO_PATH)$(DOCKER_USERNAME_PATH)$(DOCKER_BASE_IMAGE_NAME)
DOCKER_TAGGED_IMAGE_NAME := $(DOCKER_REPO)/$(REPO_NAME)/$(DOCKER_PROJECT_PATH)$(DOCKER_IMAGE_NAME):$(DOCKER_IMAGE_TAG)
DB_HOST := $(shell echo "$$DB_HOST")
DB_PORT := $(shell echo "$$DB_PORT")
DB_NAME := $(shell echo "$$DB_NAME")
DB_USER := $(shell echo "$$DB_USER")
DB_PASSWORD := $(shell echo "$$DB_PASSWORD")
SFTP_SM_HOST := $(shell echo "$$SFTP_SM_HOST")
SFTP_SM_USERNAME := $(shell echo "$$SFTP_SM_USERNAME")
SFTP_SM_PORT := $(shell echo "$$SFTP_SM_PORT")
SFTP_SM_PASSWORD := $(shell echo "$$SFTP_SM_PASSWORD")
IMAGE_STORE_URL := $(shell echo "$$IMAGE_STORE_URL")
BASE_URL := $(shell echo "$$BASE_URL")
TOKEN := $(shell echo "$$TOKEN")
EMIRATES_BASE_URL:= $(shell echo "$$EMIRATES_BASE_URL")
EMIRATES_TOKEN:= $(shell echo "$$EMIRATES_TOKEN")
APP_HOST := $(shell echo "$$APP_HOST")
APP_PORT := $(shell echo "$$APP_PORT")
AUTH_SECRET_KEY_1 := $(shell echo "$$AUTH_SECRET_KEY_1")
AUTH_SECRET_KEY_2 := $(shell echo "$$AUTH_SECRET_KEY_2")
USER_QUERY_DIR_NAME :=$(shell echo "$$USER_QUERY_DIR_NAME")
STORAGE_ACCOUNT_NAME :=$(shell echo "$$STORAGE_ACCOUNT_NAME")
STORAGE_ACCOUNT_KEY :=$(shell echo "$$STORAGE_ACCOUNT_KEY")
CONTAINER_NAME :=$(shell echo "$$CONTAINER_NAME")
CNS_ENDPOINT :=$(shell echo "$$CNS_ENDPOINT")
CNS_URL :=$(shell echo "$$CNS_URL")
KNIGHT_WATCH_ENDPOINT :=$(shell echo "$$KNIGHT_WATCH_ENDPOINT")
KNIGHT_WATCH_TENANT_AUTH :=$(shell echo "$$KNIGHT_WATCH_TENANT_AUTH")
KNIGHT_WATCH_URL :=$(shell echo "$$KNIGHT_WATCH_URL")
WELLNESS_CAMPAIGN_BUSINESS :=$(shell echo "$$WELLNESS_CAMPAIGN_BUSINESS")
GPS_CAMPAIGN_BUSINESS :=$(shell echo "$$GPS_CAMPAIGN_BUSINESS")
CAMPAIGN_ENCRYPTION_SECRET_KEY :=$(shell echo "$$CAMPAIGN_ENCRYPTION_SECRET_KEY")
CAMPAIGN_AUTH_ID :=$(shell echo "$$CAMPAIGN_AUTH_ID")
GPS_CAMPAIGN_TEMPLATE :=$(shell echo "$$GPS_CAMPAIGN_TEMPLATE")
WELLNESS_CAMPAIGN_TEMPLATE :=$(shell echo "$$WELLNESS_CAMPAIGN_TEMPLATE")
GPS_CAMPAIGN_CODE :=$(shell echo "$$GPS_CAMPAIGN_CODE")
WELLNESS_CAMPAIGN_CODE :=$(shell echo "$$WELLNESS_CAMPAIGN_CODE")
WHATSAPP_ENDPOINT :=$(shell echo "$$WHATSAPP_ENDPOINT")
APP_BASE_URL :=$(shell echo "$$APP_BASE_URL")
FIELDING_ANALYSIS_DIR_NAME :=$(shell echo "$$FIELDING_ANALYSIS_DIR_NAME")
UMS_BASE_URL :=$(shell echo "$$UMS_BASE_URL")
SMARTABASE_USER:=$(shell echo "$$SMARTABASE_USER")
SMARTABASE_APP:=$(shell echo "$$SMARTABASE_APP")
SMARTABASE_URL:=$(shell echo "$$SMARTABASE_URL")
FILE_SHARE_PATH:=$(shell echo "$$FILE_SHARE_PATH")
INGESTION_ENABLED:=$(shell echo "$$INGESTION_ENABLED")
SFTP_INGESTION_ENABLED:=$(shell echo "$$SFTP_INGESTION_ENABLED")
BUILD_ENV:=$(shell echo "$$BUILD_ENV")
SMTP_PASSWORD:=$(shell echo "$$SMTP_PASSWORD")
SMTP_EMAIL:=$(shell echo "$$SMTP_EMAIL")
SMTP_SERVER_IP:=$(shell echo "$$SMTP_SERVER_IP")
SMTP_PORT:=$(shell echo "$$SMTP_PORT")
WPL_TOKEN:=$(shell echo "$$WPL_TOKEN")
ALLOWED_HOSTS:=$(shell echo "$$ALLOWED_HOSTS")
DOCKERFILE_PATH:=$(shell echo "$$DOCKERFILE_PATH")

############################## Mandatory Targets ##############################

docker-login:
	docker login -u $(DOCKER_USERNAME) -p $(DOCKER_PASSWORD) $(DOCKER_REPO)
	@echo "Docker logged in"

package: package_docker

publish: publish_docker

package_docker:
	docker build \
		-t $(DOCKER_IMAGE_NAME):$(DOCKER_IMAGE_TAG) \
		-f $(DOCKERFILE_PATH) \
		--build-arg BASE_IMAGE=$(DOCKER_BASE_IMAGE) \
		--build-arg PIP_EXTRA_INDEX_URL=$(PIP_EXTRA_INDEX_URL) \
		--build-arg GIT_COMMIT=$(shell git log -1 --format=%h) \
		--build-arg BUILD_ENV=$(BUILD_ENV) \
		--build-arg http_proxy=$(http_proxy) \
		--build-arg https_proxy=$(https_proxy) \
		--build-arg no_proxy=$(no_proxy) \
		--build-arg DB_HOST=$(DB_HOST)\
		--build-arg DB_PORT=$(DB_PORT)\
		--build-arg DB_NAME=$(DB_NAME)\
		--build-arg DB_PASSWORD=$(DB_PASSWORD)\
		--build-arg DB_USER=$(DB_USER)\
		--build-arg SFTP_SM_HOST=$(SFTP_SM_HOST) \
		--build-arg SFTP_SM_USERNAME=$(SFTP_SM_USERNAME) \
		--build-arg SFTP_SM_PASSWORD=$(SFTP_SM_PASSWORD)\
		--build-arg SFTP_SM_PORT=$(SFTP_SM_PORT)\
		--build-arg IMAGE_STORE_URL=$(IMAGE_STORE_URL)\
		--build-arg BASE_URL=$(BASE_URL)\
		--build-arg TOKEN=$(TOKEN)\
		--build-arg APP_HOST=$(APP_HOST)\
		--build-arg CNS_ENDPOINT=$(CNS_ENDPOINT) \
		--build-arg CNS_URL=$(CNS_URL) \
		--build-arg KNIGHT_WATCH_ENDPOINT=$(KNIGHT_WATCH_ENDPOINT) \
		--build-arg KNIGHT_WATCH_TENANT_AUTH=$(KNIGHT_WATCH_TENANT_AUTH) \
		--build-arg KNIGHT_WATCH_URL=$(KNIGHT_WATCH_URL) \
		--build-arg APP_PORT=$(APP_PORT) \
		--build-arg AUTH_SECRET_KEY_1=$(AUTH_SECRET_KEY_1) \
		--build-arg AUTH_SECRET_KEY_2=$(AUTH_SECRET_KEY_2) \
		--build-arg USER_QUERY_DIR_NAME=$(USER_QUERY_DIR_NAME)\
		--build-arg STORAGE_ACCOUNT_NAME=$(STORAGE_ACCOUNT_NAME) \
		--build-arg STORAGE_ACCOUNT_KEY=$(STORAGE_ACCOUNT_KEY) \
		--build-arg WELLNESS_CAMPAIGN_BUSINESS=$(WELLNESS_CAMPAIGN_BUSINESS) \
		--build-arg GPS_CAMPAIGN_BUSINESS=$(GPS_CAMPAIGN_BUSINESS) \
		--build-arg CAMPAIGN_ENCRYPTION_SECRET_KEY=$(CAMPAIGN_ENCRYPTION_SECRET_KEY) \
		--build-arg CAMPAIGN_AUTH_ID=$(CAMPAIGN_AUTH_ID) \
		--build-arg GPS_CAMPAIGN_TEMPLATE=$(GPS_CAMPAIGN_TEMPLATE) \
		--build-arg WELLNESS_CAMPAIGN_TEMPLATE=$(WELLNESS_CAMPAIGN_TEMPLATE) \
		--build-arg GPS_CAMPAIGN_CODE=$(GPS_CAMPAIGN_CODE) \
		--build-arg WELLNESS_CAMPAIGN_CODE=$(WELLNESS_CAMPAIGN_CODE) \
		--build-arg WHATSAPP_ENDPOINT=$(WHATSAPP_ENDPOINT) \
		--build-arg APP_BASE_URL=$(APP_BASE_URL) \
		--build-arg CONTAINER_NAME=$(CONTAINER_NAME) \
		--build-arg EMIRATES_BASE_URL=$(EMIRATES_BASE_URL)\
		--build-arg UMS_BASE_URL=$(UMS_BASE_URL)\
		--build-arg EMIRATES_TOKEN=$(EMIRATES_TOKEN)\
		--build-arg FIELDING_ANALYSIS_DIR_NAME=$(FIELDING_ANALYSIS_DIR_NAME)\
		--build-arg SMARTABASE_USER=$(SMARTABASE_USER)\
		--build-arg SMARTABASE_PASSWORD=$(SMARTABASE_PASSWORD)\
		--build-arg SMARTABASE_APP=$(SMARTABASE_APP)\
		--build-arg FILE_SHARE_PATH=$(FILE_SHARE_PATH)\
		--build-arg SMARTABASE_URL=$(SMARTABASE_URL)\
		--build-arg INGESTION_ENABLED=$(INGESTION_ENABLED)\
		--build-arg BUILD_ENV=$(BUILD_ENV)\
		--build-arg SMTP_PASSWORD=$(SMTP_PASSWORD)\
		--build-arg SMTP_EMAIL=$(SMTP_EMAIL)\
		--build-arg SMTP_SERVER_IP=$(SMTP_SERVER_IP)\
		--build-arg SMTP_PORT=$(SMTP_PORT)\
		--build-arg SFTP_INGESTION_ENABLED=$(SFTP_INGESTION_ENABLED)\
		--build-arg WPL_TOKEN=$(WPL_TOKEN)\
		--build-arg ALLOWED_HOSTS=$(ALLOWED_HOSTS)\
		.

publish_docker:
	docker tag $(DOCKER_IMAGE_NAME):$(DOCKER_IMAGE_TAG) $(DOCKER_TAGGED_IMAGE_NAME)
	docker login -u $(DOCKER_USERNAME) -p $(DOCKER_PASSWORD) $(DOCKER_REPO)
	docker push $(DOCKER_TAGGED_IMAGE_NAME)