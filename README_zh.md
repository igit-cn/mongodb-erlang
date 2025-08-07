# MongoDB Erlang 驱动

[![Erlang CI](https://github.com/emqx/mongodb-erlang/actions/workflows/erlang.yml/badge.svg?branch=master)](https://github.com/emqx/mongodb-erlang/actions/workflows/erlang.yml)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Version](https://img.shields.io/badge/version-3.0.27-green.svg)](https://github.com/emqx/mongodb-erlang)

一个高性能、功能丰富的 [MongoDB](https://www.mongodb.org/) Erlang/OTP 应用程序驱动。该驱动提供了对 MongoDB 操作的全面支持，包括 CRUD 操作、事务、聚合、索引和连接池。

## 特性

- **双 API 设计**: 可选择直接连接 (`mc_worker_api`) 或池化连接 (`mongo_api`)
- **协议支持**: 自动检测并支持传统协议和现代 MongoDB 协议 (OP_MSG)
- **连接池**: 内置连接池，支持溢出管理
- **拓扑发现**: 自动 MongoDB 拓扑发现和监控，支持副本集和分片集群
- **事务支持**: 完整支持 MongoDB 事务和会话
- **身份验证**: 支持 MongoDB 身份验证机制
- **全面操作**: 支持所有主要 MongoDB 操作（CRUD、聚合、索引）
- **高性能**: 针对高吞吐量应用程序优化
- **生产就绪**: 在生产环境中经过实战测试

## 系统要求

- Erlang/OTP 18+
- MongoDB 3.0+（MongoDB 5.1+ 需要现代协议）

## 安装

### 使用 Rebar3

在 `rebar.config` 中添加：

```erlang
{deps, [
    {mongodb, {git, "https://github.com/emqx/mongodb-erlang.git", {tag, "3.0.27"}}}
]}.
```

### 使用 Erlang.mk

在 `Makefile` 中添加：

```makefile
DEPS = mongodb
dep_mongodb = git https://github.com/emqx/mongodb-erlang.git 3.0.27
```

### 手动安装

```bash
$ git clone https://github.com/emqx/mongodb-erlang.git mongodb
$ cd mongodb
$ make
```

## 快速开始

### 1. 启动应用程序

```erlang
application:ensure_all_started(mongodb).
```

### 2. 连接到 MongoDB

```erlang
%% 简单连接
Database = <<"mydb">>,
{ok, Connection} = mc_worker_api:connect([{database, Database}]).

%% 带身份验证的连接
{ok, Connection} = mc_worker_api:connect([
    {database, <<"mydb">>},
    {host, "localhost"},
    {port, 27017},
    {login, <<"username">>},
    {password, <<"password">>}
]).
```

### 3. 基本操作

```erlang
Collection = <<"users">>,

%% 插入文档
Doc = #{<<"name">> => <<"张三">>, <<"age">> => 30},
{ok, _} = mc_worker_api:insert(Connection, Collection, Doc),

%% 查找文档
{ok, Cursor} = mc_worker_api:find(Connection, Collection, #{}),
Results = mc_cursor:rest(Cursor),

%% 查找单个文档
User = mc_worker_api:find_one(Connection, Collection, #{<<"name">> => <<"张三">>}),

%% 更新文档
mc_worker_api:update(Connection, Collection, 
    #{<<"name">> => <<"张三">>}, 
    #{<<"$set">> => #{<<"age">> => 31}}),

%% 删除文档
mc_worker_api:delete(Connection, Collection, #{<<"name">> => <<"张三">>}).
```

## API 概览

该驱动提供两个主要的 API 模块：

### mc_worker_api - 直接连接 API

适用于简单应用程序或需要直接控制连接的场景：

- `mc_worker_api:connect/1` - 建立连接
- `mc_worker_api:insert/3` - 插入文档
- `mc_worker_api:find/3,4` - 查询文档
- `mc_worker_api:update/4` - 更新文档
- `mc_worker_api:delete/3` - 删除文档
- `mc_worker_api:command/2` - 执行原始命令

### mongo_api - 池化连接 API

推荐用于生产应用程序，具有自动拓扑发现功能：

- `mongo_api:connect/4` - 带拓扑发现的连接
- `mongo_api:insert/3` - 带连接池的插入
- `mongo_api:find/4,6` - 带连接池的查询
- `mongo_api:update/5` - 带连接池的更新
- `mongo_api:delete/3` - 带连接池的删除

## 协议配置

驱动会自动检测适当的 MongoDB 协议。您可以覆盖此行为：

```erlang
%% 强制使用传统协议（适用于 MongoDB < 3.6）
application:set_env(mongodb, use_legacy_protocol, true),

%% 强制使用现代 OP_MSG 协议（推荐用于 MongoDB 3.6+）
application:set_env(mongodb, use_legacy_protocol, false),

%% 每连接协议设置
{ok, Connection} = mc_worker_api:connect([
    {database, <<"mydb">>},
    {use_legacy_protocol, false}
]).
```

**注意**: MongoDB 5.1+ 已移除对传统协议的支持。

## 详细使用指南

### 直接连接 API (mc_worker_api)

#### 连接选项

`mc_worker_api:connect/1` 函数接受以下选项：

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

#### 写入模式

- **`safe`**: 等待 MongoDB 的写入确认。出错时返回 `{failure, {write_failure, Reason}}`。
- **`unsafe`**: 即发即忘的写入。更快但无错误报告。
- **`{safe, GetLastErrorParams}`**: 带自定义参数的安全模式。

#### 读取模式

- **`master`**: 仅从主服务器读取。确保数据新鲜。
- **`slave_ok`**: 允许从从服务器读取。可能返回过时数据。

#### 高级连接选项

```erlang
%% 注册工作进程
{ok, Connection} = mc_worker_api:connect([
    {database, <<"mydb">>},
    {register, my_mongo_worker}
]).

%% 身份验证
{ok, Connection} = mc_worker_api:connect([
    {database, <<"mydb">>},
    {login, <<"username">>},
    {password, <<"password">>}
]).

%% 自定义主机和端口
{ok, Connection} = mc_worker_api:connect([
    {database, <<"mydb">>},
    {host, "mongodb.example.com"},
    {port, 27017}
]).

%% 使用 next_req_fun 优化池
{ok, Connection} = mc_worker_api:connect([
    {database, <<"mydb">>},
    {next_req_fun, fun() -> poolboy:checkin(?DBPOOL, self()) end}
]).
```

### CRUD 操作

#### 插入操作

```erlang
Collection = <<"teams">>,

%% 插入单个文档
Team = #{
    <<"name">> => <<"洋基队">>,
    <<"home">> => #{<<"city">> => <<"纽约">>, <<"state">> => <<"NY">>},
    <<"league">> => <<"美国联盟">>
},
{{true, _}, InsertedDoc} = mc_worker_api:insert(Connection, Collection, Team),

%% 插入多个文档
Teams = [
    #{<<"name">> => <<"洋基队">>, <<"league">> => <<"美国联盟">>},
    #{<<"name">> => <<"大都会队">>, <<"league">> => <<"国家联盟">>},
    #{<<"name">> => <<"红袜队">>, <<"league">> => <<"美国联盟">>}
],
{{true, _}, InsertedDocs} = mc_worker_api:insert(Connection, Collection, Teams).
```

**注意**: 如果文档不包含 `_id` 字段，MongoDB 会自动生成一个。

#### 删除操作

```erlang
%% 删除所有匹配选择器的文档
mc_worker_api:delete(Connection, Collection, #{<<"league">> => <<"国家联盟">>}),

%% 删除所有文档（空选择器）
mc_worker_api:delete(Connection, Collection, #{}),

%% 仅删除一个文档
mc_worker_api:delete_one(Connection, Collection, #{<<"name">> => <<"洋基队">>}),

%% 带限制的删除
mc_worker_api:delete_limit(Connection, Collection, #{<<"league">> => <<"美国联盟">>}, 2).
```

#### 查询操作

```erlang
%% 查找所有文档
{ok, Cursor} = mc_worker_api:find(Connection, Collection, #{}),
AllDocs = mc_cursor:rest(Cursor),  % 自动关闭游标

%% 带选择器的查找
{ok, Cursor} = mc_worker_api:find(Connection, Collection, #{<<"league">> => <<"美国联盟">>}),
AmericanTeams = mc_cursor:rest(Cursor),

%% 查找单个文档
Team = mc_worker_api:find_one(Connection, Collection, #{<<"name">> => <<"洋基队">>}),

%% 带投影的查找（仅返回特定字段）
Team = mc_worker_api:find_one(Connection, Collection,
    #{<<"name">> => <<"洋基队">>},
    #{projector => #{<<"name">> => true, <<"league">> => true}}),

%% 带投影的查找（排除特定字段）
Team = mc_worker_api:find_one(Connection, Collection,
    #{<<"name">> => <<"洋基队">>},
    #{projector => #{<<"_id">> => false}}),

%% 手动游标处理
{ok, Cursor} = mc_worker_api:find(Connection, Collection, #{}),
case mc_cursor:next(Cursor) of
    error ->
        no_more_docs;
    Doc ->
        process_document(Doc),
        mc_cursor:close(Cursor)  % 重要：始终关闭游标！
end.
```

**重要**: 使用完游标后始终关闭，或使用自动关闭的 `mc_cursor:rest/1`。

#### 计数操作

```erlang
%% 计算所有文档
Total = mc_worker_api:count(Connection, Collection, #{}),

%% 带选择器的计数
AmericanCount = mc_worker_api:count(Connection, Collection, #{<<"league">> => <<"美国联盟">>}).
```

#### 更新操作

```erlang
%% 更新单个文档 - 设置字段
UpdateCmd = #{<<"$set">> => #{
    <<"league">> => <<"国家联盟">>,
    <<"city">> => <<"波士顿">>,
    <<"founded">> => 1901
}},
mc_worker_api:update(Connection, Collection,
    #{<<"name">> => <<"红袜队">>},
    UpdateCmd),

%% 更新嵌套文档字段
UpdateCmd = #{<<"$set">> => #{<<"home.city">> => <<"剑桥">>}},
mc_worker_api:update(Connection, Collection,
    #{<<"name">> => <<"红袜队">>},
    UpdateCmd),

%% 更新数组元素
UpdateCmd = #{<<"$set">> => #{
    <<"players.0">> => <<"新球员">>,
    <<"stats.wins">> => 95
}},
mc_worker_api:update(Connection, Collection,
    #{<<"name">> => <<"洋基队">>},
    UpdateCmd),

%% 递增值
UpdateCmd = #{<<"$inc">> => #{<<"wins">> => 1, <<"losses">> => -1}},
mc_worker_api:update(Connection, Collection,
    #{<<"name">> => <<"洋基队">>},
    UpdateCmd),

%% 添加到数组
UpdateCmd = #{<<"$push">> => #{<<"players">> => <<"新球员">>}},
mc_worker_api:update(Connection, Collection,
    #{<<"name">> => <<"洋基队">>},
    UpdateCmd),

%% 带 upsert 的更新（不存在则创建）
mc_worker_api:update(Connection, Collection,
    #{<<"name">> => <<"新队伍">>},
    #{<<"$set">> => #{<<"league">> => <<"美国联盟">>}},
    true,  % upsert
    false). % multi-update
```

### 索引

**注意**: `ensure_index/3` 在版本 3.0.16+ 中已弃用。请使用带 `createIndexes` 的 `command/2`。

```erlang
%% 创建简单索引（弃用方法）
mc_worker_api:ensure_index(Connection, Collection,
    #{<<"key">> => #{<<"name">> => 1}}),

%% 创建复合索引（弃用方法）
mc_worker_api:ensure_index(Connection, Collection,
    #{<<"key">> => #{<<"name">> => 1, <<"league">> => 1},
      <<"name">> => <<"name_league_idx">>}),

%% 创建唯一索引（弃用方法）
mc_worker_api:ensure_index(Connection, Collection,
    #{<<"key">> => #{<<"email">> => 1},
      <<"unique">> => true,
      <<"name">> => <<"email_unique_idx">>}),

%% 推荐：使用 createIndexes 命令
CreateIndexCmd = #{
    <<"createIndexes">> => Collection,
    <<"indexes">> => [
        #{<<"key">> => #{<<"name">> => 1}, <<"name">> => <<"name_idx">>},
        #{<<"key">> => #{<<"email">> => 1}, <<"name">> => <<"email_idx">>, <<"unique">> => true}
    ]
},
mc_worker_api:command(Connection, CreateIndexCmd).
```

### 原始命令

直接执行任何 MongoDB 命令：

```erlang
%% 获取数据库统计信息
StatsCmd = #{<<"dbStats">> => 1},
{true, Stats} = mc_worker_api:command(Connection, StatsCmd),

%% 获取集合信息
ListCollectionsCmd = #{<<"listCollections">> => 1},
{true, Collections} = mc_worker_api:command(Connection, ListCollectionsCmd),

%% 创建带选项的集合
CreateCollectionCmd = #{
    <<"create">> => <<"mycollection">>,
    <<"capped">> => true,
    <<"size">> => 100000
},
{true, Result} = mc_worker_api:command(Connection, CreateCollectionCmd).
```

### 配置和超时

```erlang
%% 设置全局连接超时
application:set_env(mongodb, mc_worker_call_timeout, 30000),

%% 设置游标超时
application:set_env(mongodb, cursor_timeout, 10000),

%% 在游标操作中使用超时
{ok, Cursor} = mc_worker_api:find(Connection, Collection, #{}),
Result = mc_cursor:next(Cursor, 5000),  % 5 秒超时
mc_cursor:close(Cursor).
```

## 池化连接 API (mongo_api)

`mongo_api` 模块提供了带自动连接池和拓扑发现的高级接口。推荐用于生产应用程序。

### 拓扑类型

```erlang
%% 单服务器
{ok, Topology} = mongo_api:connect(single, ["localhost:27017"], Options, WorkerOptions),

%% 副本集
{ok, Topology} = mongo_api:connect(rs,
    {<<"myReplicaSet">>, ["mongo1:27017", "mongo2:27017", "mongo3:27017"]},
    Options, WorkerOptions),

%% 分片集群
{ok, Topology} = mongo_api:connect(sharded,
    ["mongos1:27017", "mongos2:27017"],
    Options, WorkerOptions),

%% 自动发现（推荐）
{ok, Topology} = mongo_api:connect(unknown,
    ["mongo1:27017", "mongo2:27017"],
    Options, WorkerOptions).
```

### 拓扑选项

```erlang
Options = [
    {name, my_mongo_pool},              % 池注册名称
    {register, my_mongo_topology},      % 拓扑进程名称
    {pool_size, 10},                    % 初始池大小
    {max_overflow, 20},                 % 最大溢出工作进程
    {overflow_ttl, 1000},               % 溢出工作进程 TTL（毫秒）
    {overflow_check_period, 1000},      % 溢出检查周期（毫秒）
    {localThresholdMS, 1000},           % 服务器选择的本地阈值
    {connectTimeoutMS, 20000},          % 连接超时
    {socketTimeoutMS, 100},             % 套接字超时
    {serverSelectionTimeoutMS, 30000},  % 服务器选择超时
    {waitQueueTimeoutMS, 1000},         % 等待队列超时
    {heartbeatFrequencyMS, 10000},      % 心跳频率
    {minHeartbeatFrequencyMS, 1000},    % 最小心跳频率
    {rp_mode, primary},                 % 读偏好模式
    {rp_tags, []}                       % 读偏好标签
].

WorkerOptions = [
    {database, <<"mydb">>},
    {login, <<"username">>},
    {password, <<"password">>},
    {w_mode, safe}
].
```

### 使用 mongo_api 操作

```erlang
%% 所有操作都自动池化
Collection = <<"teams">>,

%% 插入
{{true, _}, _} = mongo_api:insert(Topology, Collection,
    #{<<"name">> => <<"巨人队">>, <<"league">> => <<"国家联盟">>}),

%% 查找
{ok, Cursor} = mongo_api:find(Topology, Collection,
    #{<<"league">> => <<"美国联盟">>},
    #{<<"name">> => true}),

%% 查找单个
Team = mongo_api:find_one(Topology, Collection,
    #{<<"name">> => <<"洋基队">>},
    #{<<"league">> => true}),

%% 更新
mongo_api:update(Topology, Collection,
    #{<<"name">> => <<"洋基队">>},
    #{<<"$set">> => #{<<"wins">> => 95}}}),

%% 删除
mongo_api:delete(Topology, Collection, #{<<"league">> => <<"国家联盟">>}),

%% 计数
Count = mongo_api:count(Topology, Collection, #{<<"league">> => <<"美国联盟">>}, 0),

%% 执行命令
{true, Result} = mongo_api:command(Topology, #{<<"dbStats">> => 1}, 5000).
```

## 事务和会话

通过会话 API 支持 MongoDB 事务：

```erlang
%% 启动会话
{ok, SessionId} = mc_worker_api:start_session(Connection),

%% 启动事务
ok = mc_worker_api:start_transaction(Connection, SessionId),

%% 在事务中执行操作
mc_worker_api:insert(Connection, Collection, Doc, #{session_id => SessionId}),
mc_worker_api:update(Connection, Collection, Selector, Update, #{session_id => SessionId}),

%% 提交事务
ok = mc_worker_api:commit_transaction(Connection),

%% 结束会话
ok = mc_worker_api:end_session(SessionId),

%% 或使用便利函数
Result = mc_worker_api:with_transaction(Connection, SessionId,
    fun() ->
        mc_worker_api:insert(Connection, Collection, Doc),
        mc_worker_api:update(Connection, Collection, Selector, Update),
        {ok, success}
    end).
```

## MongoDB 4.x+ 高级特性

### 变更流 (MongoDB 4.0+)

实时监控数据变更：

```erlang
%% 监控整个部署的变更
{ok, ChangeStream} = mc_worker_api:watch(Connection, []),

%% 监控特定集合的变更
{ok, ChangeStream} = mc_worker_api:watch_collection(Connection, <<"mydb">>, <<"users">>, []),

%% 使用管道过滤器监控
Pipeline = [#{<<"$match">> => #{<<"operationType">> => <<"insert">>}}],
{ok, ChangeStream} = mc_worker_api:watch_collection(Connection, <<"mydb">>, <<"users">>, Pipeline),

%% 获取下一个变更事件
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

%% 关闭变更流
mc_change_stream:close(ChangeStream).
```

### 增强聚合管道 (MongoDB 4.x+)

使用现代聚合操作符和阶段：

```erlang
%% 使用新操作符构建聚合管道
Pipeline = mc_aggregation:pipeline()
    |> mc_aggregation:match(#{<<"status">> => <<"active">>})
    |> mc_aggregation:add_fields(#{
        <<"fullName">> => mc_aggregation:concat([<<"$firstName">>, <<" ">>, <<"$lastName">>]),
        <<"ageGroup">> => mc_aggregation:switch(#{
            <<"branches">> => [
                #{<<"case">> => #{<<"$lt">> => [<<"$age">>, 18]}, <<"then">> => <<"未成年">>},
                #{<<"case">> => #{<<"$lt">> => [<<"$age">>, 65]}, <<"then">> => <<"成年人">>}
            ],
            <<"default">> => <<"老年人">>
        })
    })
    |> mc_aggregation:group(#{
        <<"_id">> => <<"$ageGroup">>,
        <<"count">> => #{<<"$sum">> => 1},
        <<"avgAge">> => #{<<"$avg">> => <<"$age">>}
    })
    |> mc_aggregation:sort(#{<<"count">> => -1}),

%% 执行聚合
{ok, Cursor} = mc_worker_api:aggregate(Connection, <<"users">>, Pipeline),
Results = mc_cursor:rest(Cursor).
```

### 可重试写入 (MongoDB 4.x+)

对瞬时网络错误自动重试：

```erlang
%% 带自动重试的可重试插入
{{true, _}, InsertedDocs} = mc_worker_api:retryable_insert(Connection, Collection, Documents),

%% 带自定义重试选项的可重试更新
Options = #{
    retry_writes => true,
    max_retries => 3,
    retry_delay_ms => 200,
    enhanced_retry => true
},
{true, UpdateResult} = mc_worker_api:retryable_update(Connection, Collection, Selector, Update, Options),

%% 可重试删除
{true, DeleteResult} = mc_worker_api:retryable_delete(Connection, Collection, Selector).
```

### 批量操作 (MongoDB 4.x+)

高效的批处理操作：

```erlang
%% 定义批量操作
BulkOps = [
    #{type => insert, document => #{<<"name">> => <<"张三">>, <<"age">> => 30}},
    #{type => update, filter => #{<<"name">> => <<"李四">>},
      update => #{<<"$set">> => #{<<"age">> => 25}}, upsert => true},
    #{type => delete, filter => #{<<"status">> => <<"inactive">>}, multi => true}
],

%% 执行批量写入
{ok, BulkResult} = mc_worker_api:bulk_write(Connection, Collection, BulkOps),

%% 检查结果
InsertedCount = maps:get(<<"insertedCount">>, BulkResult, 0),
ModifiedCount = maps:get(<<"modifiedCount">>, BulkResult, 0),
DeletedCount = maps:get(<<"deletedCount">>, BulkResult, 0).
```

### 索引管理 (MongoDB 4.x+)

现代索引操作：

```erlang
%% 使用现代选项创建索引
IndexSpec = #{<<"email">> => 1, <<"status">> => 1},
IndexOptions = #{
    unique => true,
    partial_filter_expression => #{<<"status">> => <<"active">>},
    name => <<"email_active_unique">>,
    background => true
},
{ok, _} = mc_worker_api:create_index_with_options(Connection, Collection, IndexSpec, IndexOptions),

%% 列出所有索引
{ok, Indexes} = mc_worker_api:list_indexes(Connection, Collection),

%% 删除特定索引
{ok, _} = mc_worker_api:drop_index(Connection, Collection, <<"email_active_unique">>).
```

### 查询优化 (MongoDB 4.x+)

使用索引提示和查询解释：

```erlang
%% 使用索引提示查找
{ok, Cursor} = mc_worker_api:find_with_hint(Connection, Collection,
    #{<<"status">> => <<"active">>},
    <<"status_1">>),

%% 解释查询执行计划
{ok, ExplanationPlan} = mc_worker_api:explain_query(Connection, Collection,
    #{<<"email">> => <<"user@example.com">>}),

%% 检查是否使用了索引
IndexUsed = maps:get([<<"executionStats">>, <<"totalDocsExamined">>], ExplanationPlan, 0) =:= 1.
```

### 集合管理 (MongoDB 4.x+)

高级集合操作：

```erlang
%% 获取详细的集合统计信息
{ok, Stats} = mc_worker_api:get_collection_stats(Connection, Collection),
DocumentCount = maps:get(<<"count">>, Stats),
StorageSize = maps:get(<<"storageSize">>, Stats),

%% 验证集合完整性
{ok, ValidationResult} = mc_worker_api:validate_collection(Connection, Collection),
IsValid = maps:get(<<"valid">>, ValidationResult, false),

%% 压缩集合（回收磁盘空间）
{ok, _} = mc_worker_api:compact_collection(Connection, Collection),

%% 重建所有索引
{ok, _} = mc_worker_api:reindex_collection(Connection, Collection).
```

## 测试

运行测试套件：

```bash
make tests
```

运行特定测试套件：

```bash
make eunit    # 单元测试
make ct       # Common Test 套件
```

## 文档

- [API 文档](http://api.mongodb.org/erlang/mongodb/) - 从源代码生成
- [MongoDB 手册](https://docs.mongodb.com/manual/) - 官方 MongoDB 文档
- [BSON 规范](http://bsonspec.org/) - BSON 格式规范

## 贡献

1. Fork 仓库
2. 创建功能分支
3. 进行更改
4. 为新功能添加测试
5. 运行测试套件
6. 提交拉取请求

## 许可证

本项目采用 Apache License 2.0 许可证 - 详见 [LICENSE](LICENSE) 文件。

## 更新日志

### 版本 3.1.0 (最新版)
- **MongoDB 4.x+ 完整支持**: 完整实现 MongoDB 4.x 和 5.x 特性
- **变更流**: 带恢复令牌的实时数据变更监控
- **增强可重试写入**: 带指数退避和详细错误处理的自动重试
- **高级聚合管道**: MongoDB 4.x+ 的新操作符和阶段
- **批量操作**: 支持混合操作类型的高效批量写入操作
- **现代索引管理**: 带部分过滤器和高级选项的增强索引创建
- **查询优化**: 索引提示、查询解释和性能分析工具
- **集合管理**: 统计信息、验证、压缩和重建索引操作
- **改进错误处理**: 更好的错误分类和重试逻辑
- **性能优化**: 管道优化和连接池改进

### 版本 3.0.27
- 协议自动检测改进
- 增强事务支持
- 错误修复和性能改进

### 版本 3.0.0
- `mongoc` 和 `mc_cursor` 的主要 API 更改
- 弃用 `ensure_index` 函数
- 添加 OP_MSG 协议支持

## 支持

- GitHub Issues: [报告错误和请求功能](https://github.com/emqx/mongodb-erlang/issues)
- 文档: [API 参考](http://api.mongodb.org/erlang/mongodb/)

---

**注意**: 该驱动正在积极维护并在生产环境中使用。有关最新更新和发布，请查看 [GitHub 仓库](https://github.com/emqx/mongodb-erlang)。
