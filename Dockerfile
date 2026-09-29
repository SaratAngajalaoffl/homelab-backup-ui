FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY *.go ./
COPY templates ./templates
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /backup-ui .

FROM gcr.io/distroless/static-debian12:nonroot
LABEL org.opencontainers.image.source="https://github.com/SaratAngajalaoffl/homelab-backup-ui"
COPY --from=build /backup-ui /backup-ui
USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/backup-ui"]
