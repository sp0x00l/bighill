# AWS API Gateway - local dev setup

## Overview

* This AWS API Gateway uses the `SAM cli` on a local dev machine, similar to the cloud env. 
* The [template](./template.yml) file defines the resources for the lambdas in the API gateway.
* The configuration includes an API lambda that proxies requests to the backend service and an authenticaiton lambda that validates an access token.

### Layout in the root lambda folder

* The [api](./api/) runs on `localhost:3000` and exposes the retained data platform endpoints, for example `v1/data/registry`. It uses the middleware pattern and can easily be extended. One such middleware handler creates an initial open-telemetry span for the incoming request.
* The [auth](./auth/) lambda validates the token in the request `Authentication` header.  

## Install

* From the root directory, run `make install-dev` in to install the local aws api-gateway dev stack.
* This will install `AWS CLI` and `SAM CLI` on your dev machine. `brew`, `make` `docker` and `golang` should be already installed by the root `scripts/install-dev`.

## Build and Run - at the root

* Run `make build` to build both binaries for the `auth` and `api` lambdas. 
* Run `make run` to start the api-gateway in the background. Use `tail -f api-debug.log` to get the verbose debug output. And finally use `make stop` to terminate the background process. The api-gateway will listen on `http://127.0.0.1:3000`.
* Run `make test` to start the backend services, the database and the api gateway, and then run a suite of end-to-end tests found in the `e2e_tests` folder. It also runs the gateway lambda unit tests. Debugging the gateway is difficult outside the unit tests, so it's best to unit test extensively.
* When developing, you will probably need to start the database and the service you're wiring up, and the gateway template in three separate terminal tabs.

```bash
cd ../database
make db-setup
```

```bash
cd ../data_registry_service
make run
```

```bash
make build
sam local start-api --log-file /dev/stdout --debug
```

* The lambdas are difficult to debug. Breakpoints cannot easily be set nor is it possible to step through the code because they run within a docker container. However, you can find the call stack trace and panics in the `api-debug.log` from the `--log-file` option in the run command.

## The template.yml

* `AWSTemplateFormatVersion` is the the version of the AWS CloudFormation template. It should always be set to "2010-09-09" for compatibility.
* `Transform: AWS::Serverless-2016-10-31` is required and indicates that this template is processed using SAM.
* `Parameters` are template and environment variable parameters.  

### Resources

* `Resources` are any AWS resource included in the gateway. We define our Lambda binaries functions, `BighillAuthFunction` and `BighillApiFunction` (but we could add other resources such as database definitions, etc).
* `CodeUri` points to the compiled Go binary located in the `build` directory. The `runtime` is `provided.al2` (Amazon Linux 2 - OS only) because we're running a binary. The `Handler: bootstrap` is the binary containing the Handler entry point (registered in the main.go file) and `bootstrap` appears to be hard naming convention.

### OpenAPI Specification (Swagger)

* `BighillApi` is an OpenAPI (Swagger) spec for our Gateway API.
* It defines our API endpoints, request/response formats and auth security.

## Services API HTTP Ports
  
* `data_registry_service` = 8081
* `data_ingestion_service` = 8086

add new services ports with last value +1
