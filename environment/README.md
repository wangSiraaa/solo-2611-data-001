# 原生环境启动

需要 Python 3.11 和 PostgreSQL 16 的服务端、客户端二进制。Linux 上应安装对应发行版的 Python venv 包和 PostgreSQL 16；macOS 可使用对应版本的本机安装。将 PostgreSQL 16 的 bin 目录加入 PATH，执行 `python3 --version` 和 `postgres --version` 核对版本。

下面是普通用户创建独立开发数据库的示例，仅用于初始化环境，不包含任务业务逻辑：

```sh
python3 -m venv .venv
mkdir -p .runtime
initdb -D "$PWD/.runtime/pgdata" --auth-local=trust --auth-host=scram-sha-256
pg_ctl -D "$PWD/.runtime/pgdata" -l "$PWD/.runtime/postgres.log" -o "-k $PWD/.runtime -c listen_addresses=''" start
createdb -h "$PWD/.runtime" meter_ingest
psql -h "$PWD/.runtime" -d meter_ingest -c 'select version();'
```

通过当前用户的 Unix socket 连接数据库，连接参数由程序的运行说明给出。停止该实例可执行 `pg_ctl -D "$PWD/.runtime/pgdata" stop`。数据库、依赖、日志及验证输出均放在忽略目录中，不提交到 Git。
