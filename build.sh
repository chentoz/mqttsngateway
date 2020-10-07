#! /bin/sh
TARGET=$(whereis mqttsngateway | cut -d":" -f2)
if [ -z "$TARGET" ]
then
    TARGET=$GOBIN/mqttsngateway
fi

echo "Target : $TARGET"

go build -v -ldflags "-X main.appVersion=`date +%Y%m%d-%H%M%S`" -o $TARGET