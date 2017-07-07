default:
	go build -o app

with_race:
	go build -race -o app

clean:
	go clean && rm -f app local.log
