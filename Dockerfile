FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
ARG SERVICE=gateway
RUN CGO_ENABLED=0 GOOS=linux go build -tags production -ldflags="-s -w" -o /out/app ./cmd/${SERVICE}

FROM gcr.io/distroless/base-debian12
WORKDIR /
COPY --from=build /out/app /app
COPY web /web
USER nonroot:nonroot
ENTRYPOINT ["/app"]
