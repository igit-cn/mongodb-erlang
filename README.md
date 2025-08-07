# MongoDB Erlang Driver

[![Erlang CI](https://github.com/emqx/mongodb-erlang/actions/workflows/erlang.yml/badge.svg?branch=master)](https://github.com/emqx/mongodb-erlang/actions/workflows/erlang.yml)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Version](https://img.shields.io/badge/version-3.0.27-green.svg)](https://github.com/emqx/mongodb-erlang)

A high-performance, feature-rich [MongoDB](https://www.mongodb.org/) driver for Erlang/OTP applications. This driver provides comprehensive support for MongoDB operations including CRUD operations, transactions, aggregation, indexing, and connection pooling.

## Features

- **Dual API Design**: Choose between direct connection (`mc_worker_api`) or pooled connections (`mongo_api`)
- **Protocol Support**: Automatic detection and support for both legacy and modern MongoDB protocols (OP_MSG)
- **Connection Pooling**: Built-in connection pooling with overflow management
- **Topology Discovery**: Automatic MongoDB topology discovery and monitoring for replica sets and sharded clusters
- **Transactions**: Full support for MongoDB transactions and sessions
- **Authentication**: Support for MongoDB authentication mechanisms
- **Comprehensive Operations**: Support for all major MongoDB operations (CRUD, aggregation, indexing)
- **High Performance**: Optimized for high-throughput applications
- **Production Ready**: Battle-tested in production environments

## Requirements

- Erlang/OTP 18+
- MongoDB 3.0+ (MongoDB 5.1+ requires modern protocol)

## Installation

### Using Rebar3

Add to your `rebar.config`:

```erlang
{deps, [
    {mongodb, {git, "https://github.com/emqx/mongodb-erlang.git", {tag, "3.0.27"}}}
]}.
```

### Using Erlang.mk

Add to your `Makefile`:

```makefile
DEPS = mongodb
dep_mongodb = git https://github.com/emqx/mongodb-erlang.git 3.0.27
```

### Manual Installation

```bash
$ git clone https://github.com/emqx/mongodb-erlang.git mongodb
$ cd mongodb
$ make
```

## Quick Start

### 1. Start the Application

```erlang
application:ensure_all_started(mongodb).
```

### 2. Connect to MongoDB

```erlang
%% Simple connection
Database = <<"mydb">>,
{ok, Connection} = mc_worker_api:connect([{database, Database}]).

%% Connection with authentication
{ok, Connection} = mc_worker_api:connect([
    {database, <<"mydb">>},
    {host, "localhost"},
    {port, 27017},
    {login, <<"username">>},
    {password, <<"password">>}
]).
```

### 3. Basic Operations

```erlang
Collection = <<"users">>,

%% Insert document
Doc = #{<<"name">> => <<"John Doe">>, <<"age">> => 30},
{ok, _} = mc_worker_api:insert(Connection, Collection, Doc),

%% Find documents
{ok, Cursor} = mc_worker_api:find(Connection, Collection, #{}),
Results = mc_cursor:rest(Cursor),

%% Find one document
User = mc_worker_api:find_one(Connection, Collection, #{<<"name">> => <<"John Doe">>}),

%% Update document
mc_worker_api:update(Connection, Collection,
    #{<<"name">> => <<"John Doe">>},
    #{<<"$set">> => #{<<"age">> => 31}}),

%% Delete document
mc_worker_api:delete(Connection, Collection, #{<<"name">> => <<"John Doe">>}).
```

## API Overview

This driver provides two main API modules:

### mc_worker_api - Direct Connection API

Best for simple applications or when you need direct control over connections:

- `mc_worker_api:connect/1` - Establish connection
- `mc_worker_api:insert/3` - Insert documents
- `mc_worker_api:find/3,4` - Query documents
- `mc_worker_api:update/4` - Update documents
- `mc_worker_api:delete/3` - Delete documents
- `mc_worker_api:command/2` - Execute raw commands

### mongo_api - Pooled Connection API

Recommended for production applications with automatic topology discovery:

- `mongo_api:connect/4` - Connect with topology discovery
- `mongo_api:insert/3` - Insert with connection pooling
- `mongo_api:find/4,6` - Query with connection pooling
- `mongo_api:update/5` - Update with connection pooling
- `mongo_api:delete/3` - Delete with connection pooling

## Protocol Configuration

The driver automatically detects the appropriate MongoDB protocol. You can override this behavior:

```erlang
%% Force legacy protocol (for MongoDB < 3.6)
application:set_env(mongodb, use_legacy_protocol, true),

%% Force modern OP_MSG protocol (recommended for MongoDB 3.6+)
application:set_env(mongodb, use_legacy_protocol, false),

%% Per-connection protocol setting
{ok, Connection} = mc_worker_api:connect([
    {database, <<"mydb">>},
    {use_legacy_protocol, false}
]).
```

**Note**: MongoDB 5.1+ has removed support for the legacy protocol.

## Detailed Usage Guide

### Direct Connection API (mc_worker_api)

#### Connection Options

The `mc_worker_api:connect/1` function accepts the following options:

```erlang
-type arg() :: {database, database()}
    | {login, binary()}
    | {password, binary()}
    | {w_mode, write_mode()}
    | {r_mode, read_mode()}
    | {host, list()}
    | {port, integer()}
    | {register, atom() | fun()}
    | {next_req_fun, fun()}
    | {use_legacy_protocol, boolean()}.
```

#### Write Modes

- **`safe`**: Waits for write confirmation from MongoDB. Returns `{failure, {write_failure, Reason}}` on error.
- **`unsafe`**: Fire-and-forget writes. Faster but no error reporting.
- **`{safe, GetLastErrorParams}`**: Safe mode with custom parameters.

#### Read Modes

- **`master`**: Read only from primary server. Ensures fresh data.
- **`slave_ok`**: Allow reads from secondary servers. May return stale data.

#### Advanced Connection Options

```erlang
%% Register the worker process
{ok, Connection} = mc_worker_api:connect([
    {database, <<"mydb">>},
    {register, my_mongo_worker}
]).

%% Authentication
{ok, Connection} = mc_worker_api:connect([
    {database, <<"mydb">>},
    {login, <<"username">>},
    {password, <<"password">>}
]).

%% Custom host and port
{ok, Connection} = mc_worker_api:connect([
    {database, <<"mydb">>},
    {host, "mongodb.example.com"},
    {port, 27017}
]).

%% Pool optimization with next_req_fun
{ok, Connection} = mc_worker_api:connect([
    {database, <<"mydb">>},
    {next_req_fun, fun() -> poolboy:checkin(?DBPOOL, self()) end}
]).
```

### CRUD Operations

#### Insert Operations

```erlang
Collection = <<"teams">>,

%% Insert single document
Team = #{
    <<"name">> => <<"Yankees">>,
    <<"home">> => #{<<"city">> => <<"New York">>, <<"state">> => <<"NY">>},
    <<"league">> => <<"American">>
},
{{true, _}, InsertedDoc} = mc_worker_api:insert(Connection, Collection, Team),

%% Insert multiple documents
Teams = [
    #{<<"name">> => <<"Yankees">>, <<"league">> => <<"American">>},
    #{<<"name">> => <<"Mets">>, <<"league">> => <<"National">>},
    #{<<"name">> => <<"Red Sox">>, <<"league">> => <<"American">>}
],
{{true, _}, InsertedDocs} = mc_worker_api:insert(Connection, Collection, Teams).
```

**Note**: If documents don't contain an `_id` field, MongoDB will automatically generate one.

#### Delete Operations

```erlang
%% Delete all documents matching selector
mc_worker_api:delete(Connection, Collection, #{<<"league">> => <<"National">>}),

%% Delete all documents (empty selector)
mc_worker_api:delete(Connection, Collection, #{}),

%% Delete only one document
mc_worker_api:delete_one(Connection, Collection, #{<<"name">> => <<"Yankees">>}),

%% Delete with limit
mc_worker_api:delete_limit(Connection, Collection, #{<<"league">> => <<"American">>}, 2).
```

#### Query Operations

```erlang
%% Find all documents
{ok, Cursor} = mc_worker_api:find(Connection, Collection, #{}),
AllDocs = mc_cursor:rest(Cursor),  % Automatically closes cursor

%% Find with selector
{ok, Cursor} = mc_worker_api:find(Connection, Collection, #{<<"league">> => <<"American">>}),
AmericanTeams = mc_cursor:rest(Cursor),

%% Find one document
Team = mc_worker_api:find_one(Connection, Collection, #{<<"name">> => <<"Yankees">>}),

%% Find with projection (only return specific fields)
Team = mc_worker_api:find_one(Connection, Collection,
    #{<<"name">> => <<"Yankees">>},
    #{projector => #{<<"name">> => true, <<"league">> => true}}),

%% Find with projection (exclude specific fields)
Team = mc_worker_api:find_one(Connection, Collection,
    #{<<"name">> => <<"Yankees">>},
    #{projector => #{<<"_id">> => false}}),

%% Manual cursor handling
{ok, Cursor} = mc_worker_api:find(Connection, Collection, #{}),
case mc_cursor:next(Cursor) of
    error ->
        no_more_docs;
    Doc ->
        process_document(Doc),
        mc_cursor:close(Cursor)  % Important: Always close cursors!
end.
```

**Important**: Always close cursors when done, or use `mc_cursor:rest/1` which closes automatically.

#### Count Operations

```erlang
%% Count all documents
Total = mc_worker_api:count(Connection, Collection, #{}),

%% Count with selector
AmericanCount = mc_worker_api:count(Connection, Collection, #{<<"league">> => <<"American">>}).
```

#### Update Operations

```erlang
%% Update single document - set fields
UpdateCmd = #{<<"$set">> => #{
    <<"league">> => <<"National">>,
    <<"city">> => <<"Boston">>,
    <<"founded">> => 1901
}},
mc_worker_api:update(Connection, Collection,
    #{<<"name">> => <<"Red Sox">>},
    UpdateCmd),

%% Update nested document fields
UpdateCmd = #{<<"$set">> => #{<<"home.city">> => <<"Cambridge">>}},
mc_worker_api:update(Connection, Collection,
    #{<<"name">> => <<"Red Sox">>},
    UpdateCmd),

%% Update array elements
UpdateCmd = #{<<"$set">> => #{
    <<"players.0">> => <<"New Player">>,
    <<"stats.wins">> => 95
}},
mc_worker_api:update(Connection, Collection,
    #{<<"name">> => <<"Yankees">>},
    UpdateCmd),

%% Increment values
UpdateCmd = #{<<"$inc">> => #{<<"wins">> => 1, <<"losses">> => -1}},
mc_worker_api:update(Connection, Collection,
    #{<<"name">> => <<"Yankees">>},
    UpdateCmd),

%% Add to array
UpdateCmd = #{<<"$push">> => #{<<"players">> => <<"New Player">>}},
mc_worker_api:update(Connection, Collection,
    #{<<"name">> => <<"Yankees">>},
    UpdateCmd),

%% Update with upsert (create if not exists)
mc_worker_api:update(Connection, Collection,
    #{<<"name">> => <<"New Team">>},
    #{<<"$set">> => #{<<"league">> => <<"American">>}},
    true,  % upsert
    false). % multi-update
```

### Indexing

**Note**: `ensure_index/3` is deprecated in version 3.0.16+. Use `command/2` with `createIndexes` instead.

```erlang
%% Create simple index (deprecated method)
mc_worker_api:ensure_index(Connection, Collection,
    #{<<"key">> => #{<<"name">> => 1}}),

%% Create compound index (deprecated method)
mc_worker_api:ensure_index(Connection, Collection,
    #{<<"key">> => #{<<"name">> => 1, <<"league">> => 1},
      <<"name">> => <<"name_league_idx">>}),

%% Create unique index (deprecated method)
mc_worker_api:ensure_index(Connection, Collection,
    #{<<"key">> => #{<<"email">> => 1},
      <<"unique">> => true,
      <<"name">> => <<"email_unique_idx">>}),

%% Recommended: Use createIndexes command
CreateIndexCmd = #{
    <<"createIndexes">> => Collection,
    <<"indexes">> => [
        #{<<"key">> => #{<<"name">> => 1}, <<"name">> => <<"name_idx">>},
        #{<<"key">> => #{<<"email">> => 1}, <<"name">> => <<"email_idx">>, <<"unique">> => true}
    ]
},
mc_worker_api:command(Connection, CreateIndexCmd).
```

### Raw Commands

Execute any MongoDB command directly:

```erlang
%% Get database statistics
StatsCmd = #{<<"dbStats">> => 1},
{true, Stats} = mc_worker_api:command(Connection, StatsCmd),

%% Get collection information
ListCollectionsCmd = #{<<"listCollections">> => 1},
{true, Collections} = mc_worker_api:command(Connection, ListCollectionsCmd),

%% Create collection with options
CreateCollectionCmd = #{
    <<"create">> => <<"mycollection">>,
    <<"capped">> => true,
    <<"size">> => 100000
},
{true, Result} = mc_worker_api:command(Connection, CreateCollectionCmd).
```

### Configuration and Timeouts

```erlang
%% Set global connection timeout
application:set_env(mongodb, mc_worker_call_timeout, 30000),

%% Set cursor timeout
application:set_env(mongodb, cursor_timeout, 10000),

%% Use timeout in cursor operations
{ok, Cursor} = mc_worker_api:find(Connection, Collection, #{}),
Result = mc_cursor:next(Cursor, 5000),  % 5 second timeout
mc_cursor:close(Cursor).
```

## Pooled Connection API (mongo_api)

The `mongo_api` module provides a high-level interface with automatic connection pooling and topology discovery. This is recommended for production applications.

### Topology Types

```erlang
%% Single server
{ok, Topology} = mongo_api:connect(single, ["localhost:27017"], Options, WorkerOptions),

%% Replica set
{ok, Topology} = mongo_api:connect(rs,
    {<<"myReplicaSet">>, ["mongo1:27017", "mongo2:27017", "mongo3:27017"]},
    Options, WorkerOptions),

%% Sharded cluster
{ok, Topology} = mongo_api:connect(sharded,
    ["mongos1:27017", "mongos2:27017"],
    Options, WorkerOptions),

%% Auto-discovery (recommended)
{ok, Topology} = mongo_api:connect(unknown,
    ["mongo1:27017", "mongo2:27017"],
    Options, WorkerOptions).
```

### Topology Options

```erlang
Options = [
    {name, my_mongo_pool},              % Pool registration name
    {register, my_mongo_topology},      % Topology process name
    {pool_size, 10},                    % Initial pool size
    {max_overflow, 20},                 % Maximum overflow workers
    {overflow_ttl, 1000},               % Overflow worker TTL (ms)
    {overflow_check_period, 1000},      % Overflow check period (ms)
    {localThresholdMS, 1000},           % Local threshold for server selection
    {connectTimeoutMS, 20000},          % Connection timeout
    {socketTimeoutMS, 100},             % Socket timeout
    {serverSelectionTimeoutMS, 30000},  % Server selection timeout
    {waitQueueTimeoutMS, 1000},         % Wait queue timeout
    {heartbeatFrequencyMS, 10000},      % Heartbeat frequency
    {minHeartbeatFrequencyMS, 1000},    % Minimum heartbeat frequency
    {rp_mode, primary},                 % Read preference mode
    {rp_tags, []}                       % Read preference tags
].

WorkerOptions = [
    {database, <<"mydb">>},
    {login, <<"username">>},
    {password, <<"password">>},
    {w_mode, safe}
].
```

### Using mongo_api Operations

```erlang
%% All operations are automatically pooled
Collection = <<"teams">>,

%% Insert
{{true, _}, _} = mongo_api:insert(Topology, Collection,
    #{<<"name">> => <<"Giants">>, <<"league">> => <<"National">>}),

%% Find
{ok, Cursor} = mongo_api:find(Topology, Collection,
    #{<<"league">> => <<"American">>},
    #{<<"name">> => true}),

%% Find one
Team = mongo_api:find_one(Topology, Collection,
    #{<<"name">> => <<"Yankees">>},
    #{<<"league">> => true}),

%% Update
mongo_api:update(Topology, Collection,
    #{<<"name">> => <<"Yankees">>},
    #{<<"$set">> => #{<<"wins">> => 95}}}),

%% Delete
mongo_api:delete(Topology, Collection, #{<<"league">> => <<"National">>}),

%% Count
Count = mongo_api:count(Topology, Collection, #{<<"league">> => <<"American">>}, 0),

%% Execute command
{true, Result} = mongo_api:command(Topology, #{<<"dbStats">> => 1}, 5000).
```

## Transactions and Sessions

MongoDB transactions are supported through the session API:

```erlang
%% Start a session
{ok, SessionId} = mc_worker_api:start_session(Connection),

%% Start a transaction
ok = mc_worker_api:start_transaction(Connection, SessionId),

%% Perform operations within transaction
mc_worker_api:insert(Connection, Collection, Doc, #{session_id => SessionId}),
mc_worker_api:update(Connection, Collection, Selector, Update, #{session_id => SessionId}),

%% Commit transaction
ok = mc_worker_api:commit_transaction(Connection),

%% End session
ok = mc_worker_api:end_session(SessionId),

%% Or use the convenience function
Result = mc_worker_api:with_transaction(Connection, SessionId,
    fun() ->
        mc_worker_api:insert(Connection, Collection, Doc),
        mc_worker_api:update(Connection, Collection, Selector, Update),
        {ok, success}
    end).
```

## MongoDB 4.x+ Advanced Features

### Change Streams (MongoDB 4.0+)

Monitor real-time changes to your data:

```erlang
%% Watch changes on entire deployment
{ok, ChangeStream} = mc_worker_api:watch(Connection, []),

%% Watch changes on specific collection
{ok, ChangeStream} = mc_worker_api:watch_collection(Connection, <<"mydb">>, <<"users">>, []),

%% Watch with pipeline filter
Pipeline = [#{<<"$match">> => #{<<"operationType">> => <<"insert">>}}],
{ok, ChangeStream} = mc_worker_api:watch_collection(Connection, <<"mydb">>, <<"users">>, Pipeline),

%% Get next change event
case mc_change_stream:next(ChangeStream, 5000) of
    {ok, ChangeEvent} ->
        OperationType = maps:get(<<"operationType">>, ChangeEvent),
        FullDocument = maps:get(<<"fullDocument">>, ChangeEvent, undefined),
        process_change(OperationType, FullDocument);
    timeout ->
        no_changes;
    {error, Reason} ->
        handle_error(Reason)
end,

%% Close change stream
mc_change_stream:close(ChangeStream).
```

### Enhanced Aggregation Pipeline (MongoDB 4.x+)

Use modern aggregation operators and stages:

```erlang
%% Build aggregation pipeline with new operators
Pipeline = mc_aggregation:pipeline()
    |> mc_aggregation:match(#{<<"status">> => <<"active">>})
    |> mc_aggregation:add_fields(#{
        <<"fullName">> => mc_aggregation:concat([<<"$firstName">>, <<" ">>, <<"$lastName">>]),
        <<"ageGroup">> => mc_aggregation:switch(#{
            <<"branches">> => [
                #{<<"case">> => #{<<"$lt">> => [<<"$age">>, 18]}, <<"then">> => <<"minor">>},
                #{<<"case">> => #{<<"$lt">> => [<<"$age">>, 65]}, <<"then">> => <<"adult">>}
            ],
            <<"default">> => <<"senior">>
        })
    })
    |> mc_aggregation:group(#{
        <<"_id">> => <<"$ageGroup">>,
        <<"count">> => #{<<"$sum">> => 1},
        <<"avgAge">> => #{<<"$avg">> => <<"$age">>}
    })
    |> mc_aggregation:sort(#{<<"count">> => -1}),

%% Execute aggregation
{ok, Cursor} = mc_worker_api:aggregate(Connection, <<"users">>, Pipeline),
Results = mc_cursor:rest(Cursor).
```

### Retryable Writes (MongoDB 4.x+)

Automatic retry for transient network errors:

```erlang
%% Retryable insert with automatic retry
{{true, _}, InsertedDocs} = mc_worker_api:retryable_insert(Connection, Collection, Documents),

%% Retryable update with custom retry options
Options = #{
    retry_writes => true,
    max_retries => 3,
    retry_delay_ms => 200,
    enhanced_retry => true
},
{true, UpdateResult} = mc_worker_api:retryable_update(Connection, Collection, Selector, Update, Options),

%% Retryable delete
{true, DeleteResult} = mc_worker_api:retryable_delete(Connection, Collection, Selector).
```

### Bulk Operations (MongoDB 4.x+)

Efficient batch operations:

```erlang
%% Define bulk operations
BulkOps = [
    #{type => insert, document => #{<<"name">> => <<"John">>, <<"age">> => 30}},
    #{type => update, filter => #{<<"name">> => <<"Jane">>},
      update => #{<<"$set">> => #{<<"age">> => 25}}, upsert => true},
    #{type => delete, filter => #{<<"status">> => <<"inactive">>}, multi => true}
],

%% Execute bulk write
{ok, BulkResult} = mc_worker_api:bulk_write(Connection, Collection, BulkOps),

%% Check results
InsertedCount = maps:get(<<"insertedCount">>, BulkResult, 0),
ModifiedCount = maps:get(<<"modifiedCount">>, BulkResult, 0),
DeletedCount = maps:get(<<"deletedCount">>, BulkResult, 0).
```

### Index Management (MongoDB 4.x+)

Modern index operations:

```erlang
%% Create index with modern options
IndexSpec = #{<<"email">> => 1, <<"status">> => 1},
IndexOptions = #{
    unique => true,
    partial_filter_expression => #{<<"status">> => <<"active">>},
    name => <<"email_active_unique">>,
    background => true
},
{ok, _} = mc_worker_api:create_index_with_options(Connection, Collection, IndexSpec, IndexOptions),

%% List all indexes
{ok, Indexes} = mc_worker_api:list_indexes(Connection, Collection),

%% Drop specific index
{ok, _} = mc_worker_api:drop_index(Connection, Collection, <<"email_active_unique">>).
```

### Query Optimization (MongoDB 4.x+)

Use index hints and query explanation:

```erlang
%% Find with index hint
{ok, Cursor} = mc_worker_api:find_with_hint(Connection, Collection,
    #{<<"status">> => <<"active">>},
    <<"status_1">>),

%% Explain query execution plan
{ok, ExplanationPlan} = mc_worker_api:explain_query(Connection, Collection,
    #{<<"email">> => <<"user@example.com">>}),

%% Check if index was used
IndexUsed = maps:get([<<"executionStats">>, <<"totalDocsExamined">>], ExplanationPlan, 0) =:= 1.
```

### Collection Management (MongoDB 4.x+)

Advanced collection operations:

```erlang
%% Get detailed collection statistics
{ok, Stats} = mc_worker_api:get_collection_stats(Connection, Collection),
DocumentCount = maps:get(<<"count">>, Stats),
StorageSize = maps:get(<<"storageSize">>, Stats),

%% Validate collection integrity
{ok, ValidationResult} = mc_worker_api:validate_collection(Connection, Collection),
IsValid = maps:get(<<"valid">>, ValidationResult, false),

%% Compact collection (reclaim disk space)
{ok, _} = mc_worker_api:compact_collection(Connection, Collection),

%% Rebuild all indexes
{ok, _} = mc_worker_api:reindex_collection(Connection, Collection).
```

## Testing

Run the test suite:

```bash
make tests
```

Run specific test suites:

```bash
make eunit    # Unit tests
make ct       # Common Test suites
```

## Documentation

- [API Documentation](http://api.mongodb.org/erlang/mongodb/) - Generated from source code
- [MongoDB Manual](https://docs.mongodb.com/manual/) - Official MongoDB documentation
- [BSON Specification](http://bsonspec.org/) - BSON format specification

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Run the test suite
6. Submit a pull request

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Changelog

### Version 3.1.0 (Latest)
- **MongoDB 4.x+ Full Support**: Complete implementation of MongoDB 4.x and 5.x features
- **Change Streams**: Real-time data change monitoring with resume tokens
- **Enhanced Retryable Writes**: Automatic retry with exponential backoff and detailed error handling
- **Advanced Aggregation Pipeline**: New operators and stages from MongoDB 4.x+
- **Bulk Operations**: Efficient batch write operations with mixed operation types
- **Modern Index Management**: Enhanced index creation with partial filters and advanced options
- **Query Optimization**: Index hints, query explanation, and performance analysis tools
- **Collection Management**: Statistics, validation, compaction, and reindexing operations
- **Improved Error Handling**: Better error categorization and retry logic
- **Performance Optimizations**: Pipeline optimization and connection pooling improvements

### Version 3.0.27
- Protocol auto-detection improvements
- Enhanced transaction support
- Bug fixes and performance improvements

### Version 3.0.0
- Major API changes for `mongoc` and `mc_cursor`
- Deprecated `ensure_index` function
- Added OP_MSG protocol support

## Support

- GitHub Issues: [Report bugs and request features](https://github.com/emqx/mongodb-erlang/issues)
- Documentation: [API Reference](http://api.mongodb.org/erlang/mongodb/)

---

**Note**: This driver is actively maintained and used in production environments. For the latest updates and releases, check the [GitHub repository](https://github.com/emqx/mongodb-erlang).
