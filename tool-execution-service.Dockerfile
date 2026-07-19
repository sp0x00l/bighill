ARG TARGETARCH=amd64
ARG BUILD_VERSION_REQUIRED=0.0.0

FROM --platform=linux/${TARGETARCH} golang:alpine AS builder

RUN addgroup -S tool_execution_service_server_group && \
    adduser -S tool_execution_service_server_user -G tool_execution_service_server_group

WORKDIR $GOPATH/src/shared_lib
COPY ./shared_lib .

WORKDIR $GOPATH/src/data_contracts/build/protobufs
COPY ./data_contracts/build/protobufs .

WORKDIR $GOPATH/src/tool_execution_service
COPY ./tool_execution_service .

RUN rm -f go.mod go.sum && go mod init tool_execution_service
RUN go mod edit -go=1.26.4
RUN go mod edit -replace lib/shared_lib=../shared_lib
RUN go mod edit -replace lib/data_contracts_lib=../data_contracts/build/protobufs
RUN go mod tidy
RUN go mod download

RUN apk add --no-cache gcc musl-dev
RUN CGO_ENABLED=1 GOOS=linux GOARCH=${TARGETARCH} go build -tags musl -ldflags="-w -s -X 'main.Version=${BUILD_VERSION_REQUIRED}'" -o /go/bin/tool_execution_service

FROM --platform=linux/${TARGETARCH} golang:alpine
LABEL bighill="services"

COPY --from=builder /go/bin/tool_execution_service /go/bin/tool_execution_service
COPY --from=builder /etc/group /etc/group
COPY --from=builder /etc/passwd /etc/passwd

WORKDIR /usr/local/src
COPY ./scripts/docker/services/tool-execution-service-entrypoint.sh .
RUN apk update && apk add --no-cache bash curl && rm -rf /var/cache/apk/*

USER tool_execution_service_server_user

ENTRYPOINT ["sh", "/usr/local/src/tool-execution-service-entrypoint.sh"]
