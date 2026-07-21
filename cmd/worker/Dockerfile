# Build
FROM golang:1.25-alpine AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -o /bin/worker ./cmd/worker

# Run
FROM alpine:3.22
RUN adduser -D app
USER app
COPY --from=build /bin/worker /bin/worker
ENTRYPOINT ["/bin/worker"]
