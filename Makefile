all:
	cd src && make
	cd priv && make

shell:
	make all
	erl -pa ebin