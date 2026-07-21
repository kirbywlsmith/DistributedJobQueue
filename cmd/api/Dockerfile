# Build
FROM golang:1.25-alpine AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -o /bin/api ./cmd/api

# Run
FROM alpine:3.22
RUN adduser -D app
USER app
COPY --from=build /bin/api /bin/api
EXPOSE 8080
ENTRYPOINT ["/bin/api"]
