# Build mode: "full" builds pdf_extractor_lib from source, "prebuilt" uses checked/downloaded artifacts.
ARG BUILD_MODE=full
ARG TARGETARCH=amd64
ARG BUILD_VERSION_REQUIRED=0.0.0

FROM --platform=linux/${TARGETARCH} golang:alpine AS pdf_builder_full
ARG TARGETARCH

RUN addgroup -S feature_materializer_service_server_group && \
    adduser -S feature_materializer_service_server_user -G feature_materializer_service_server_group

RUN apk add --no-cache build-base cmake coreutils gcc g++ libstdc++ libstdc++-dev linux-headers pkgconf poppler-cpp-dev

WORKDIR $GOPATH/src/shared_lib
COPY ./shared_lib .

WORKDIR $GOPATH/src/pdf_extractor_lib
COPY ./pdf_extractor_lib .

ENV CPP_ROOT=$GOPATH/src/pdf_extractor_lib/cpp

RUN rm -rf $GOPATH/src/pdf_extractor_lib/cpp/build/bin && \
    mkdir -p $GOPATH/src/pdf_extractor_lib/cpp/build/bin

WORKDIR $GOPATH/src/pdf_extractor_lib/cpp
RUN cmake -DCMAKE_BUILD_TYPE=Release \
    -DCPP_ROOT=$CPP_ROOT \
    -DCMAKE_CXX_COMPILER=g++ \
    -DCMAKE_C_COMPILER=gcc \
    .
RUN make -j$(nproc)

FROM --platform=linux/${TARGETARCH} golang:alpine AS pdf_builder_prebuilt
ARG TARGETARCH

RUN addgroup -S feature_materializer_service_server_group && \
    adduser -S feature_materializer_service_server_user -G feature_materializer_service_server_group

WORKDIR $GOPATH/src/shared_lib
COPY ./shared_lib .

WORKDIR $GOPATH/src/pdf_extractor_lib
COPY ./pdf_extractor_lib .

RUN mkdir -p $GOPATH/src/pdf_extractor_lib/cpp/build/bin
COPY ./pdf_extractor_lib/cpp/prebuilt/lib/libgo_pdf_extractor_lib.a $GOPATH/src/pdf_extractor_lib/cpp/build/bin/

FROM pdf_builder_${BUILD_MODE} AS pdf_builder

FROM --platform=linux/${TARGETARCH} golang:alpine AS go_builder
ARG TARGETARCH
ARG BUILD_VERSION_REQUIRED=0.0.0

COPY --from=pdf_builder $GOPATH/src/shared_lib $GOPATH/src/shared_lib
COPY --from=pdf_builder $GOPATH/src/pdf_extractor_lib $GOPATH/src/pdf_extractor_lib
COPY --from=pdf_builder /etc/group /etc/group
COPY --from=pdf_builder /etc/passwd /etc/passwd

RUN apk add --no-cache build-base gcc g++ libstdc++ libstdc++-dev linux-headers musl-dev pkgconf poppler-cpp-dev

WORKDIR $GOPATH/src/shared_lib
COPY ./shared_lib .

WORKDIR $GOPATH/src/data_contracts/build/protobufs
COPY ./data_contracts/build/protobufs .

WORKDIR $GOPATH/src/feature_materializer_service
COPY ./feature_materializer_service .

RUN rm -f go.mod && go mod init feature_materializer_service
RUN go mod edit -replace lib/shared_lib=../shared_lib
RUN go mod edit -replace lib/data_contracts_lib=../data_contracts/build/protobufs
RUN go mod edit -replace lib/pdf_extractor_lib=../pdf_extractor_lib
RUN go get -d -v

RUN CGO_ENABLED=1 GOOS=linux GOARCH=${TARGETARCH} go build -tags musl -ldflags="-w -s" -o /go/bin/feature_materializer_service

FROM --platform=linux/${TARGETARCH} golang:alpine
LABEL bighill="services"

COPY --from=go_builder /go/bin/feature_materializer_service /go/bin/feature_materializer_service
COPY --from=go_builder /etc/group /etc/group
COPY --from=go_builder /etc/passwd /etc/passwd

WORKDIR /usr/local/src
COPY ./scripts/docker/services/feature-materializer-service-entrypoint.sh .
RUN apk update && apk add --no-cache bash curl libstdc++ poppler-cpp && rm -rf /var/cache/apk/*

USER feature_materializer_service_server_user

ENTRYPOINT ["sh", "/usr/local/src/feature-materializer-service-entrypoint.sh"]
