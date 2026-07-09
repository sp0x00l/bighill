FROM golang:alpine AS builder

ARG BUILD_VERSION_REQUIRED

WORKDIR /app
COPY ./api_gateway/template.yml ./

WORKDIR $GOPATH/src/shared_lib
COPY ./shared_lib .

# RUN GOARCH=$(uname -m | sed 's/x86_64/amd64/' | sed 's/aarch64/arm64/')

WORKDIR $GOPATH/src/api_gateway/lambda/api
COPY ./api_gateway/lambda/api .
RUN rm -rf go.mod go.sum
RUN rm -f go.mod && go mod init api
RUN go mod edit -replace lib/shared_lib=$GOPATH/src/shared_lib
RUN go get -d -v
RUN mkdir -p /build/api_binary
RUN GOARCH=amd64 CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s -X 'main.Version=${BUILD_VERSION_REQUIRED}'" -o /build/api_binary/bootstrap


WORKDIR $GOPATH/src/api_gateway/lambda/auth
COPY ./api_gateway/lambda/auth .
RUN rm -rf go.mod go.sum
RUN rm -f go.mod && go mod init auth
RUN go mod edit -replace lib/shared_lib=$GOPATH/src/shared_lib
RUN go get -d -v
RUN mkdir -p /build/auth_binary
RUN GOARCH=amd64 CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s -X 'main.Version=${BUILD_VERSION_REQUIRED}'" -o /build/auth_binary/bootstrap


RUN rm -rf $GOPATH/src/api_gateway/lambda

FROM public.ecr.aws/sam/build-provided.al2023:latest-arm64
LABEL bighill="services"

COPY --from=builder /build/api_binary/bootstrap /build/api_binary/bootstrap
COPY --from=builder /build/auth_binary/bootstrap /build/auth_binary/bootstrap
COPY --from=builder /app/template.yml /app/template.yml

# Prevent the AWS SAM CLI from sending telemetry data to the regional AWS serverless telemetry endpoint
ENV SAM_CLI_TELEMETRY=0

EXPOSE 3000
CMD ["sam", "local", "start-api", "--host", "0.0.0.0", "--template", "/app/template.yml"]
