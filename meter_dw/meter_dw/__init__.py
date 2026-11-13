"""电表分钟数据增量入仓程序。

模块划分：
- config:   连接配置与常量
- db:       连接管理、迁移、咨询锁（单实例执行者）
- parsing:  JSONL 行解析、校验、规范化摘要
- ingest:   分批事务入仓、检查点、冲突隔离、日报增量维护
- cli:      命令行入口（migrate/import/resume/status/report/trace）
"""

__version__ = "1.0.0"
