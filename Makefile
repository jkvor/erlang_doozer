all: 
	rebar get-deps compile

clean:
	rm -rf ebin/*.beam deps/*

