FROM golang:1.25.7-alpine@sha256:f6751d823c26342f9506c03797d2527668d095b0a15f1862cddb4d927a7a4ced AS build
ARG TARGETARCH
WORKDIR /src
COPY tests/fixtures/execution_runtime_fixture/main.go ./main.go
RUN CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH} go build -trimpath -ldflags="-s -w" -o /out/fixture ./main.go

FROM scratch
COPY --from=build /out/fixture /fixture
USER 65532:65532
ENTRYPOINT ["/fixture"]
