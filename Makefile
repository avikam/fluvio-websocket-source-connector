test:
	bats ./tests/websocket-test.bats

test_fluvio_install:
	fluvio version
	fluvio topic list
	fluvio topic create foobar
	sleep 3
	echo foo | fluvio produce foobar
	fluvio consume foobar -B -d
