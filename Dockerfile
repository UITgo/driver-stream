FROM golang:1.22 AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/driver-stream ./cmd/server

FROM gcr.io/distroless/base-debian12
WORKDIR /
COPY --from=build /out/driver-stream /driver-stream
COPY internal/assign/claim.lua /internal/assign/claim.lua
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/driver-stream"]
