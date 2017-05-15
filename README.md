1.Dependent
This is need install nanomsg  https://github.com/nanomsg/nanomsg

2.Build
```
make && make install

create extension nanomsgtopdb; 
```

3.Parameter
```
	nanomsg_work.ip_address 	default is 0.0.0.0
	nanomsg_work.port		default is 9999.
	nanomsg_work.nanomsg_max_tuples_a_transaction    default is 1000 rows insert stream.
	nanomsg_work.isrec		default is off.
	nanomsg_work.nanomsg_works  	default is 1 process work in pipelineDB. max value is 10.
	nanomsg_work.format		default is text froamt,maybe last support bytea .
```

4.About generic_stream
This is nanomsg main stream.everyone data put this stream.you can used transform into other stream.
Default format is text,need transform to json (create continuous view cv as select data::json from generic_stream;).

5.Send data to nanomsg
```
create continuous view cv_nanomsg as select data from generic_stream;

cd test
make

./send_msg
```

```
pipeline=# select * from cv_nanomsg;
           data
--------------------------
 Hello ,from nanomsg msg.
(1 row) 
```
 
