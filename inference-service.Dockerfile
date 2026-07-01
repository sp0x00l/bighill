ARG TARGETARCH=amd64
ARG BUILD_VERSION_REQUIRED=0.0.0

FROM --platform=linux/${TARGETARCH} golang:alpine AS builder

RUN addgroup -S inference_service_server_group && \
    adduser -S inference_service_server_user -G inference_service_server_group

WORKDIR $GOPATH/src/shared_lib
COPY ./shared_lib .

WORKDIR $GOPATH/src/data_contracts/build/protobufs
COPY ./data_contracts/build/protobufs .

WORKDIR $GOPATH/src/inference_service
COPY ./inference_service .

RUN rm -f go.mod && go mod init inference_service
RUN go mod edit -replace lib/shared_lib=../shared_lib
RUN go mod edit -replace lib/data_contracts_lib=../data_contracts/build/protobufs
RUN go get -d -v

RUN apk add --no-cache gcc musl-dev
RUN CGO_ENABLED=1 GOOS=linux GOARCH=${TARGETARCH} go build -tags musl -ldflags="-w -s" -o /go/bin/inference_service

FROM --platform=linux/${TARGETARCH} golang:alpine
LABEL bighill="services"

COPY --from=builder /go/bin/inference_service /go/bin/inference_service
COPY --from=builder /etc/group /etc/group
COPY --from=builder /etc/passwd /etc/passwd

WORKDIR /usr/local/src
COPY ./scripts/docker/services/inference-service-entrypoint.sh .
RUN apk update && apk add --no-cache bash curl postgresql-client && rm -rf /var/cache/apk/*

USER inference_service_server_user

ENTRYPOINT ["sh", "/usr/local/src/inference-service-entrypoint.sh"]
