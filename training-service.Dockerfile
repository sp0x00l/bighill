ARG TARGETARCH=amd64
ARG BUILD_VERSION_REQUIRED=0.0.0

FROM --platform=linux/${TARGETARCH} golang:alpine AS builder

RUN addgroup -S training_service_server_group && \
    adduser -S training_service_server_user -G training_service_server_group

WORKDIR $GOPATH/src/shared_lib
COPY ./shared_lib .

WORKDIR $GOPATH/src/training_service
COPY ./training_service .

RUN rm -f go.mod && go mod init training_service
RUN go mod edit -replace lib/shared_lib=../shared_lib
RUN go get -d -v

RUN apk add --no-cache gcc musl-dev
RUN CGO_ENABLED=1 GOOS=linux GOARCH=${TARGETARCH} go build -tags musl -ldflags="-w -s" -o /go/bin/training_service

FROM --platform=linux/${TARGETARCH} golang:alpine
LABEL bighill="services"

COPY --from=builder /go/bin/training_service /go/bin/training_service
COPY --from=builder /etc/group /etc/group
COPY --from=builder /etc/passwd /etc/passwd

WORKDIR /usr/local/src
COPY ./scripts/docker/services/training-service-entrypoint.sh .
RUN apk update && apk add --no-cache bash curl && rm -rf /var/cache/apk/*

USER training_service_server_user

ENTRYPOINT ["sh", "/usr/local/src/training-service-entrypoint.sh"]
