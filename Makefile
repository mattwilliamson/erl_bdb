all:
	cd src && make
	cd priv && make

shell:
	make all
	erl -pa ebin

clean:
	cd src && make
	cd priv && make
