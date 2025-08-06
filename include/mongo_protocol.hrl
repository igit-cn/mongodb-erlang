% Wire protocol message types (records)

-ifndef(MONGO_PROTOCOL).
-define(MONGO_PROTOCOL, true).

-define(GS2_HEADER, <<"n,,">>).

-type colldb() :: collection() | {database(), collection()}.
-type collection() :: binary() | atom(). % without db prefix
-type database() :: binary() | atom().
-type command() :: insert | update | delete.
-type read_concern_level() :: local | available | majority | linearizable | snapshot.


%% write
-record(insert, {
  collection :: colldb(),
  documents :: [map() | bson:document()]
}).

-record(update, {
  collection :: colldb(),
  upsert = false :: boolean(),
  multiupdate = false :: boolean(),
  selector :: mc_worker_api:selector(),
  updater :: bson:document() | mc_worker_api:modifier()
}).

-record(delete, {
  collection :: colldb(),
  singleremove = false :: boolean(),
  selector :: mc_worker_api:selector()
}).

%% read
-record('query', {
  collection :: colldb(),
  tailablecursor = false :: boolean(),
  slaveok = false :: boolean(),
  sok_overriden = false :: boolean(),
  nocursortimeout = false :: boolean(),
  awaitdata = false :: boolean(),
  skip = 0 :: mc_worker_api:skip(),
  batchsize = 0 :: mc_worker_api:batchsize(),
  selector :: mc_worker_api:selector(),
  projector = #{} :: mc_worker_api:projector()
}).

-record(op_msg_write_op, {
  command :: command(),
  collection :: colldb(),
  database :: undefined | mc_worker_api:database(),
  extra_fields = [] :: bson:document() | nonempty_list({binary(),any()}),
  documents_name = <<"documents">> :: bson:utf8(),
  documents = [] :: any()
}).

-record(op_msg_response, {
  response_doc :: map()
}).

-record(op_msg_command, {
  database :: undefined | mc_worker_api:database(),
  command_doc :: bson:document() | nonempty_list({binary(),any()})
}).


-record(getmore, {
  collection :: colldb(),
  batchsize = 0 :: mc_worker_api:batchsize(),
  cursorid :: mc_worker_api:cursorid()
}).

%% system
-record(ensure_index, {
  collection :: colldb(),
  index_spec
}).

-record(conn_state, {
  write_mode = unsafe :: mc_worker_api:write_mode(),
  read_mode = master :: mc_worker_api:read_mode(),
  database :: mc_worker_api:database(),
  auth_source :: mc_worker_api:database(),
  read_concern_level :: undefined | read_concern_level(),
  session_id :: undefined | binary()
}).
-type conn_state() :: #conn_state{}.

%% Session and Transaction records
-record(session, {
  id :: binary(),
  cluster_time :: undefined | bson:document(),
  operation_time :: undefined | bson:timestamp(),
  implicit :: boolean(),
  server_session :: undefined | map()
}).
-type session() :: #session{}.

-record(transaction_state, {
  state :: undefined | starting | in_progress | committed | aborted,
  options :: map(),
  read_concern :: undefined | map(),
  write_concern :: undefined | map(),
  read_preference :: undefined | map(),
  recovery_token :: undefined | bson:document()
}).
-type transaction_state() :: #transaction_state{}.

-record(transaction_options, {
  read_concern :: undefined | map(),
  write_concern :: undefined | map(),
  read_preference :: undefined | map(),
  max_commit_time_ms :: undefined | integer()
}).
-type transaction_options() :: #transaction_options{}.

-record(killcursor, {
  cursorids :: [mc_worker_api:cursorid()]
}).

-record(reply, {
  cursornotfound :: boolean(),
  queryerror :: boolean(),
  awaitcapable :: boolean() | undefined,
  cursorid :: mc_worker_api:cursorid(),
  startingfrom :: integer() | undefined,
  documents :: [map()]
}).
-endif.
