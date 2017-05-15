#include <string.h>
#include <stdio.h>
#include "postgres.h"
#include "utils/elog.h"
#include "nanomsg/nn.h"
#include "nanomsg/pipeline.h"
#include <unistd.h>
#include "miscadmin.h"
#include "storage/proc.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "executor/spi.h"
#include "commands/trigger.h"
#include "catalog/pg_type.h"
#include "utils/timestamp.h"
#include "utils/rel.h"
#include "utils/date.h"
#include "utils/datetime.h"
#include "utils/nabstime.h"
#include "access/xact.h"
#include "access/htup_details.h"
#include "utils/syscache.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/lsyscache.h"
#include "catalog/pg_proc.h"
#include "catalog/namespace.h"
#include "utils/tuplestore.h"
#include "commands/async.h"
#include "foreign/fdwapi.h"
#include "nodes/pg_list.h"
#include "nodes/makefuncs.h"
#include "pipeline/executor.h"
#include "pipeline/stream.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

static volatile sig_atomic_t got_sighup = false;
static volatile sig_atomic_t got_sigterm = false;
volatile sig_atomic_t got_nanomsg_job = false;
uint64_t nanomsg_spout_count;
uint64_t nanomsg_spout_bytes;

static MemoryContext nanomsgrec_ctx;

extern void nanomsg_worker(Datum main_arg);
extern void nanomsg_main(Datum main_arg);
static void run_background_nanomsg(void);

/*
 * nanomsg config
 * */
static char* ip_address = "0.0.0.0";
static char* database = "pipeline";
static int port = 9999;
static bool isrec = false;
static int nanomsg_works = 1;
static char* format = "text";
static int nanomsg_max_tuples_a_transaction = 1000;

static void worker_spi_sighup(SIGNAL_ARGS)
{

        int save_errno = errno;
        got_sighup = true;
        if (MyProc)
                SetLatch(&MyProc->procLatch);
        errno = save_errno;

}

static void worker_spi_sigterm(SIGNAL_ARGS)
{

        int save_errno = errno;
        got_sigterm = true;
        if (MyProc)
                SetLatch(&MyProc->procLatch);
        errno = save_errno;

}


/*
 *nanomsg exit
 * */
static void nanomsg_exit(int code)
{
        if (nanomsgrec_ctx) {
                MemoryContextDelete(nanomsgrec_ctx);
                nanomsgrec_ctx  = NULL;
        }
        proc_exit(code);
}

/*
 *init config
 * */
void static 
getNanomsgRecConfig(){

	DefineCustomStringVariable("nanomsg_work.ip_address",
                                                           "Nanomsg receive ip address.",
                                                           NULL,
                                                           &ip_address,
                                                           "0.0.0.0",
                                                           PGC_POSTMASTER,
                                                           0,
                                                           NULL,
                                                           NULL,
                                                           NULL);
	DefineCustomStringVariable("nanomsg_work.database",
                                                           "Nanomsg receive database.",
                                                           NULL,
                                                           &database,
                                                           "pipeline",
                                                           PGC_POSTMASTER,
                                                           0,
                                                           NULL,
                                                           NULL,
                                                           NULL);

	DefineCustomStringVariable("nanomsg_work.format",
                                                           "Nanomsg receive format.",
                                                           NULL,
                                                           &format,
                                                           "text",//default text,  bytea & text.
                                                           PGC_POSTMASTER,
                                                           0,
                                                           NULL,
                                                           NULL,
                                                           NULL);


	DefineCustomIntVariable("nanomsg_work.port",
                                                        "Nanomsg receive ip port.",
                                                        NULL,
                                                        &port,
                                                        9999,
                                                        1,
                                                        65535,
                                                        PGC_POSTMASTER,
                                                        0,
                                                        NULL,
                                                        NULL,
                                                        NULL);	

	 DefineCustomIntVariable("nanomsg_work.nanomsg_max_tuples_a_transaction",
                                                        "Nanomsg receive max tuple in a transaction.",
                                                        NULL,
                                                        &nanomsg_max_tuples_a_transaction,
                                                        1000,
                                                        1,
                                                        10000,
                                                        PGC_POSTMASTER,
                                                        0,
                                                        NULL,
                                                        NULL,
                                                        NULL);


	DefineCustomBoolVariable("nanomsg_work.isrec",
                "Nanomsg process is execute.",
              	NULL,
		 &isrec,
               	false,
		PGC_POSTMASTER,
                0,
                NULL,
                NULL,
                NULL);

	DefineCustomIntVariable("nanomsg_work.nanomsg_works",
                                                        "Nanomsg receive process number.",
                                                        NULL,
                                                        &nanomsg_works,
                                                        1,
                                                        1,
                                                        10,// 10
                                                        PGC_POSTMASTER,
                                                        0,
                                                        NULL,
                                                        NULL,
                                                        NULL);	
	
}

/*
 *get stream desc,default column is data
 * */

        static TupleDesc
get_generic_stream_desc()
{
        Oid typeid = TEXTOID;
        TupleDesc desc = CreateTemplateTupleDesc(1, false);
        AttrNumber attno = 1;
        TupleDescInitEntry(desc, attno,
                        "data",
                        typeid,
                        -1,
                        0);
        TupleDescInitEntryCollation(desc,
                        attno, InvalidOid);
        return desc;
}

/*nanomsg recevie process
 * */
void nanomsg_worker(Datum main_arg){
	int ret;
	char endpoint[64] = {0};
	int socket;
	int timeout;
	char *schema;
	char *streamname;
	RangeVar *nanomsgvar;
	TupleDesc desc;
	HeapTuple *tups;
	MemoryContext old_mem_ctx;

	schema = "public";
	streamname = "generic_stream";

	pqsignal(SIGHUP, worker_spi_sighup);
    	pqsignal(SIGTERM, worker_spi_sigterm);
	
	BackgroundWorkerUnblockSignals();
	BackgroundWorkerInitializeConnection(database, NULL);
	StartTransactionCommand();
	
	old_mem_ctx = MemoryContextSwitchTo(TopMemoryContext);
    	nanomsgvar = makeRangeVar(schema, streamname, -1);

     	tups = palloc(nanomsg_max_tuples_a_transaction * sizeof(HeapTuple));

	desc = get_generic_stream_desc();

	MemoryContextSwitchTo(old_mem_ctx);
	socket = nn_socket(AF_SP, NN_PULL);
	timeout = 1000000;//in millisec

	nanomsg_spout_count = 0;
	nanomsg_spout_bytes = 0;
	nn_setsockopt(socket, NN_SOL_SOCKET, NN_RCVTIMEO, &timeout, sizeof(timeout));

	sprintf(endpoint,"ipc:///tmp/nanomsg.%d",port);

	ret = nn_connect(socket, endpoint);
    
	if (ret < 0) {
                elog(ERROR, "unable to nn_connect %s, err:%s", endpoint, nn_strerror(ret));
    	}
    	elog(LOG, "nanomsg receive worker started.");

	nanomsgrec_ctx = AllocSetContextCreate(TopMemoryContext, "NanomsgRecCxt",
                    ALLOCSET_DEFAULT_MINSIZE,
                    ALLOCSET_DEFAULT_INITSIZE,
                    ALLOCSET_DEFAULT_MAXSIZE);

	while (!got_sigterm)
	{
		int count;
		int ntups;
		int rc;
		void *buf;
		bool nulls[1];
		Datum values[1];
		Relation rel;

		MemoryContextSwitchTo(nanomsgrec_ctx);
		CommitTransactionCommand();
		StartTransactionCommand();


		MemSet(tups, 0, nanomsg_max_tuples_a_transaction * sizeof(HeapTuple));

		count = 0;
		ntups = 0;
		while(count++ < nanomsg_max_tuples_a_transaction) {
			int size;
			size = nn_recv(socket, &buf, -1, 0);
			if (size > 0 ) {
				size_t sz;
				size_t required;
				char *ptr;

				sz = size ;
				required = sz + VARHDRSZ;
				ptr = (char*)palloc0(required);
				memcpy( VARDATA(ptr), (char*)buf, sz );
				SET_VARSIZE(ptr, required);

				values[0] = PointerGetDatum(ptr);
				nulls[0] = false;

				tups[ntups++] = heap_form_tuple(desc, values, nulls);
				nn_freemsg(buf);
			}
		}

		rel = heap_openrv_extended(nanomsgvar,RowExclusiveLock,true);
		CopyIntoStream(rel, desc, tups, ntups);
		heap_close(rel, RowExclusiveLock);
		MemoryContextReset(nanomsgrec_ctx);

		rc = WaitLatch(&MyProc->procLatch,
				WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
				0L);
		ResetLatch(&MyProc->procLatch);
		if (got_sighup)
			proc_exit(1);

		if (rc & WL_POSTMASTER_DEATH)
			nanomsg_exit(1);

	}
	CommitTransactionCommand();
	StartTransactionCommand();
	nanomsg_exit(0);
}


/*
 *nanomsg main process
 * */
void nanomsg_main(Datum main_arg){
	int listen_sock;
	int push_sock;
	char endpoint[64];
	int rc;
	int ret;

	pqsignal(SIGHUP, worker_spi_sighup);
        pqsignal(SIGTERM, worker_spi_sigterm);
	BackgroundWorkerUnblockSignals();

	MemoryContextSwitchTo(TopMemoryContext);
	
	nanomsgrec_ctx = AllocSetContextCreate(TopMemoryContext, "NanomsgRecCxt",
                        ALLOCSET_DEFAULT_MINSIZE,
                        ALLOCSET_DEFAULT_INITSIZE,
                        ALLOCSET_DEFAULT_MAXSIZE);
	MemoryContextSwitchTo(nanomsgrec_ctx);

	listen_sock = nn_socket(AF_SP_RAW, NN_PULL);
	push_sock = nn_socket(AF_SP_RAW, NN_PUSH);

	sprintf(endpoint,"tcp://%s:%d",ip_address,port);	

	ret = nn_bind(listen_sock, endpoint);

	if (ret >= 0) {
		elog(LOG, "message server listened on:%s", endpoint);
	}
	else {
		elog(ERROR, "unable to start message server on:%s, err:%s", endpoint, nn_strerror(ret));
	}


	sprintf(endpoint,"ipc:///tmp/nanomsg.%d",port);
	ret = nn_bind(push_sock, endpoint);
	if (ret >= 0) {
		elog(LOG, "message server listened on:%s", endpoint);
	}
	else {
		elog(ERROR, "unable to start message server on:%s, err:%s", endpoint, nn_strerror(ret));
	}

        elog(LOG, "nanomsg receive initialized.");

	 /* begin loop */
        while (!got_sigterm)
        {
                rc = WaitLatch(&MyProc->procLatch,
                                WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
                                0L);
                ResetLatch(&MyProc->procLatch);
                /* In case of a SIGHUP, exit and get restarted */
                if (got_sighup)
                        proc_exit(1);
                /* Emergency bailout if postmaster has died */
                if (rc & WL_POSTMASTER_DEATH) {
                        nanomsg_exit(1);
                }
                ret = nn_device(listen_sock, push_sock);
                if(ret < 0)
                {
                        elog(LOG, "nn_device err:%s", nn_strerror(errno));
                }
        }


        nanomsg_exit(0);
}


static void
run_background_nanomsg(void)
{
        BackgroundWorker worker;
        int i;

        strcpy(worker.bgw_name, "nanomsg receive main");

        worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION | BGWORKER_IS_CONT_QUERY_PROC;
        worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
        worker.bgw_main = nanomsg_main;
        worker.bgw_notify_pid = 0;
        worker.bgw_restart_time = 1; /* recover in 1s */
        worker.bgw_main_arg = (Datum)NULL;//PointerGetDatum(proc);

	RegisterBackgroundWorker(&worker);

                for(i=0; i<nanomsg_works; i++)
                {
			char workname[20];
                        MemSet(&worker, 0, sizeof(worker));

			sprintf(workname,"nanomsg worker%d",i+1);
                        strcpy(worker.bgw_name, workname);

                        worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION | BGWORKER_IS_CONT_QUERY_PROC;
                        worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
                        worker.bgw_main = nanomsg_worker;
                        worker.bgw_notify_pid = 0;
                        worker.bgw_restart_time = 1; /* recover in 1s */
                        worker.bgw_main_arg = (Datum)NULL;//PointerGetDatum(proc);

			 RegisterBackgroundWorker(&worker);
                }
}

void            _PG_init(void);

void _PG_init(void){
	if (!process_shared_preload_libraries_in_progress)
		return;	

	elog(LOG,"%s\n","nanomsg receive background process is start.");

	getNanomsgRecConfig();

	//is true 
	if(isrec)
		run_background_nanomsg();

	elog(LOG,"%s\n","nanomsg receive background process is finish.");
}
