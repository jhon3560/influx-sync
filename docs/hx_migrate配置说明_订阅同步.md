# hx_migrate 配置说明（订阅同步场景）

> 适用场景：InfluxDB 订阅 → TP202（隔离）→ InfluxDB 写入。
> 本文只讲"订阅读取 + TP202 转发 + InfluxDB 写入"这一条链路，其他功能（RTDB/文件/104/Modbus 等）不涉及。
> 内容基于官方《表迁移程序HX_MIGRATE_PROC配置文件和测试样例说明》+ 现场实测验证。

## 一、链路与文件关系

```
103（隔离前，发送侧）                         171（隔离后，接收侧）
┌─────────────────────────────┐              ┌─────────────────────────────┐
│ InfluxDB 源库(hisdb)         │              │                             │
│   └─ 订阅推送 ──18099──>    │              │                             │
│ hx_migrate_proc             │  TP202/HX6000│ hx_migrate_proc             │
│   channel 6 (read,订阅)     │ ────18898───>│   channel 8 (server,收包)    │
│   ├─ 写 dst_db_id 指向库     │              │   └─ 写 dst_db_id 指向库      │
│   └─ 随机发往 1 个 client 通道│              │     → HXScada               │
└─────────────────────────────┘              └─────────────────────────────┘
```

| 文件 | 作用 | 必配 |
|---|---|---|
| `conf/hx_migrate.conf` | 全局参数（分包、批间隔、加密向量） | 是 |
| `conf/db.conf` | 数据库索引表（db_序号=文件名） | 是 |
| `conf/db/xxx.conf` | 每个库一个文件（源库/目标库/TP202 通道） | 是 |
| `conf/channel.csv` | 通道定义（读/写/收发，最核心） | 是 |
| `conf/table_def.csv` | 各表 tag 字段定义（写库解析用） | 是 |
| `conf/channel/X.cond` | 额外查询条件（INFLUXDB 非订阅模式用） | 否 |
| `watch_dog.sh` | 守护进程（进程死了自动拉起） | 建议 |

## 二、配置详解

### 1. hx_migrate.conf（全局）

```ini
operate_type=no                 # TP202 收发主线程开关：send/recv/no。
                                # ★ 实测订阅场景配 no 即可，收发由 channel 类型驱动
server_ip=203.0.113.161         # 本机管理用 IP（订阅场景可不关注）
server_port=6668                # 同上
split_pkg_max_point_num=50      # 分包最大测点个数（发送侧）
                                # ★ 实测有效范围 1~5000，超 5000 程序自动钳制为 5000
                                # ★ 字段名写错（如写成 split_pkg_max_point_num=xxx 之外的
                                #   名字）时程序静默用缺省值 100，不报错，务必核对字段名
batch_interval_time=50          # 批量扫描间隔（毫秒）。越小越实时，越大每包越多
iv=LD5VhR/jGNgFmA4wuO082w==     # 密码加密初始向量（与 db 密码加密时一致）
back_dir=/data/hx_migrate/bakData  # 备份目录
back_days=1                     # 备份保留天数
```

### 2. db.conf（库索引）

```ini
db_count=100                 # 索引条目数（可大于实际使用数）
db_2=2.conf                  # 序号 2 对应文件 2.conf
db_7=tp202_7_client.conf     # 序号 7 对应 TP202 client
db_8=tp202_8_server.conf     # 序号 8 对应 TP202 server
db_11=11.conf                # 序号 11 对应订阅源
```

### 3. db/xxx.conf（每个库一个文件）

```ini
# 订阅源库（INFLUXDB_SUBS）
db_id=11
db_ip=127.0.0.1
db_port=18099                # ★ 订阅 HTTP 监听端口，必须与 CREATE SUBSCRIPTION
                             #   的 DESTINATIONS 地址完全一致
db_type=INFLUXDB_SUBS        # 订阅读取类型

# TP202 client 通道（发送侧，一个通道一个文件）
db_id=7
db_ip=198.51.100.171        # 对端（接收侧）IP
db_port=18898                # 对端 TP202 监听端口
db_type=TP202

# TP202 server 通道（接收侧）
db_id=8
db_ip=127.0.0.1
db_port=18898                # ★ 本机监听端口
db_type=TP202

# InfluxDB 目标库（接收侧）
db_id=2
db_ip=127.0.0.1
db_username=root
db_password=DCKiUJQDvGhtjWAafBPKtA==   # ★ 加密后的密码，生成：hx_migrate_proc -e 明文
db_port=11911
db_name=HXScada
db_type=INFLUXDB
```

### 4. channel.csv（核心，逐列说明）

表头（14 列）：
`channel_id,channel_name,channel_type,src_db_id,table_name,starttime,endtime,time_interval(sec),dst_db_id,dst_table_name,dst_channel_id,is_valid,store_type,send_all_dst_channel`

**发送侧（103）示例**：

```
6,influxdb-thread1,read,11,hisdb,,,2,2,,7|8,1,,0
7,tp202-thread1,client,7,,,,,2,,,1,,0
8,tp202-thread1,client,7,,,,,2,,,1,,0
```

| 列 | channel 6（read） | channel 7/8（client） |
|---|---|---|
| channel_type | read（读取） | client（TP202 发送） |
| src_db_id | 11（订阅源库） | 7（TP202 client 库） |
| table_name | **hisdb**（★ 只同步该表） | 空 |
| time_interval | 2（秒，读取间隔） | 2 |
| dst_db_id | 2（★ 同时写入本地库） | 空 |
| dst_channel_id | 7\|8（★ 转发给这些通道） | 空 |
| is_valid | 1（启用） | 1 |
| send_all_dst_channel | **0**（★ 随机发 1 个） | 0 |

**接收侧（171）示例**：

```
8,tp202-thread2,server,8,,,,,2,2,,1,1,,
2,influxdb-thread1,write,2,,,,,,,0,1,,
```

| 列 | channel 8（server） | channel 2（write） |
|---|---|---|
| channel_type | server（TP202 接收） | write（写库） |
| src_db_id | 8（TP202 server 库） | 2（目标库） |
| dst_db_id | **2**（★ 收到数据写 2 号库） | 空 |
| dst_channel_id | 1（转发，无通道则忽略） | 0 |
| is_valid | 1 | 1 |

### 5. table_def.csv（表的 tag 定义）

```csv
#table_name,column_name,column_type,column_data_type,
hdb,prj,tag,string,
hdb,test,tag,string,
hdb,module,tag,string,
telemetry,plant,tag,string,
telemetry,point,tag,string,
```

- 每个要同步的表，**把它的 tag 字段全部列出来**（field 可不列）
- 写库解析时按此区分 tag/field

### 6. 订阅配置（InfluxDB 端）

```sql
CREATE SUBSCRIPTION hx_sub ON hisdb.autogen DESTINATIONS ALL 'http://127.0.0.1:18099'
```

- 端口 = 订阅源库 db/xxx.conf 的 db_port
- ★ 清库（DROP DATABASE）会连带删除该库上的订阅，重建库后需重新创建

## 三、容易踩的坑（重点）

1. **读通道 table_name 是过滤条件，不是摆设**
   订阅会把源库**所有表**的写入推过来，读通道只处理 `table_name` 指定的表，**其他表的数据直接丢弃**。同步哪个表就写哪个表名，写错/漏写 = 数据静默丢失。

2. **写库靠 dst_db_id，转发靠 dst_channel_id，两码事**
   - read/server 通道的 `dst_db_id` = 收到数据直接写这个库
   - `dst_channel_id` = 转给其他通道（client 通道转发用）
   - 接收侧 server 通道即使 `dst_channel_id` 指向不存在的通道，**只要 dst_db_id 配对了就能写库**

3. **TP202 client 断线不自动重连**
   对端（接收侧）进程重启/网络中断后，client 连接进入 CLOSE-WAIT 状态**不会自动重连**，期间数据全部丢弃。**对端重启后必须重启本端 hx_migrate**（kill 后由 watch_dog 拉起）。

4. **目标库 DROP/重启后写库失效**
   目标 InfluxDB 重启或库被删后，写库会失败且**不一定有错误日志**，需重启 hx_migrate 恢复。

5. **split_pkg_max_point_num 的坑**
   - 字段名写错 → 静默用缺省 100
   - 配置 >5000 → 程序钳制为 5000（注释里的"max value 1000"是过时的）

6. **channel.csv 是 CRLF 行尾**
   用 sed 按行尾匹配替换经常失败，**建议用 Python 二进制替换**（参考附录）。

7. **密码必须加密**
   `hx_migrate_proc -e 明文` 生成密文填入 db_password，且 iv 要与生成时一致。

8. **订阅端口必须与订阅语句一致**
   源库 db 配置的 db_port（如 18099）必须等于 `CREATE SUBSCRIPTION ... 'http://127.0.0.1:18099'` 里的端口，否则收不到数据。

9. **多 client 通道是"轮换"不是"并行"**
   `send_all_dst_channel=0` 时数据随机发往 1 个通道（实测各通道轮换），**不会同时用多条连接并行发**——提升吞吐要靠大包（调大 split_pkg / batch_interval_time），不是加通道。

10. **写库后目标库可能出现 2 条几乎相同的记录**（时间戳差几十纳秒）
    实测单条小包偶发（未影响压测 count 验证），观察确认即可。

## 四、最小可用配置（订阅同步，两台）

**103（发送侧）**：
- `db/11.conf`：INFLUXDB_SUBS，127.0.0.1:18099
- `db/tp202_7_client.conf`：TP202，对端 IP:18898
- `channel.csv`：
  ```
  6,influxdb-thread1,read,11,hisdb,,,2,2,,7,1,,0
  7,tp202-thread1,client,7,,,,,2,,,1,,0
  ```
- `table_def.csv`：定义源表 tag
- 启动：`./watch_dog.sh &`（守护拉起 hx_migrate_proc）

**171（接收侧）**：
- `db/tp202_8_server.conf`：TP202，127.0.0.1:18898
- `db/2.conf`：INFLUXDB，目标库 HXScada
- `channel.csv`：
  ```
  8,tp202-thread2,server,8,,,,,2,2,,1,1,,
  2,influxdb-thread1,write,2,,,,,,,0,1,,
  ```
- `table_def.csv`：定义目标表 tag
- 启动：`./watch_dog.sh &`

## 五、常用运维

```bash
# 重启（watch_dog 会自动拉起）
killall hx_migrate_proc

# 日志
tail -f /data/hx_migrate/log/hx_migrate.info.log    # 通道收发/写库
tail -f /data/hx_migrate/log/hx_migrate.debug.log   # 订阅 raw data/解析
tail -f /data/hx_migrate/log/hx_migrate.error.log   # 错误

# 验证连通
ss -tn | grep 18898      # 发送侧看 ESTAB；接收侧看 LISTEN

# 改 CRLF 文件（Python 方式）
python3 -c "p='/opt/hx_migrate/conf/channel.csv'; s=open(p,'rb').read();
s=s.replace(b'旧串', b'新串'); open(p,'wb').write(s)"
```

## 六、订阅同步链路速查

```
写源库 → InfluxDB 订阅推送 → 读通道(按 table_name 过滤) → 写 dst_db_id 库
      → 随机 1 个 client 通道 → TP202/HX6000 包 → server 通道
      → 写 dst_db_id 库（目标 HXScada）→ 完成
```

排查顺序：源库订阅在不在 → 读通道日志有没有 raw data → 发送侧有没有 send queue → 两侧连接是否 ESTAB → 接收侧有没有 ProcRS/parse → 目标库有没有数据。
