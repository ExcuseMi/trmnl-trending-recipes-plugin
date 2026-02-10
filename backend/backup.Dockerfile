FROM alpine:3.20

RUN apk add --no-cache sqlite

COPY backup.sh /backup.sh
RUN chmod +x /backup.sh

CMD ["/backup.sh"]
