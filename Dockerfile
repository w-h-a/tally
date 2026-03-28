FROM golang:1.24-alpine AS build 
                                                                                                                               
WORKDIR /src                                                                                                                                                   

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -o /tally ./cmd/tally/

FROM scratch AS final

COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /tally /tally

ENTRYPOINT ["/tally"]
