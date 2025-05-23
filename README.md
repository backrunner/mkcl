# mkcl

基于原版 MKCL 的改进版本，支持进度展示 / 连接池。

## 概念

名词定义：帖子链

> 帖子及其引用、回复的帖子以及被引用、被回复帖子所构成的集合

名词定义：关联用户

> 某用户关注的用户或关注该用户的用户

全部符合以下条件（包括但不限于）的帖子将被删除:

* 不是本地用户发出的
* 帖子链中没有本地用户及其关联用户参与
* 帖子链中没有置顶帖子
* 帖子链中所有帖子均不包含投票
* 帖子链中所有帖子均没有被收藏
* 帖子链中没有帖子发布时间超过清理时限

符合以下条件的媒体文件将被删除

* 符合删除条件的帖子链所引用的
* 仅被引用小于等于一次
* 没有在本地（包括对象存储）实际存储文件

## 使用方法

### 前置准备

```bash
pip install -r requirements.txt
```

为了你的数据库跑得快一点，请使用下面的索引：

```sql
-- 基本索引
CREATE INDEX IF NOT EXISTS "idx_note_id_composite" ON note (id, "userId", "userHost", "renoteId", "replyId");
CREATE INDEX IF NOT EXISTS "idx_note_renote_reply" ON note ("renoteId", "replyId");
CREATE INDEX IF NOT EXISTS "idx_note_fileids" ON note USING gin ("fileIds");

-- 相关表的索引
CREATE INDEX IF NOT EXISTS "idx_note_reaction_noteid" ON note_reaction("noteId");
CREATE INDEX IF NOT EXISTS "idx_note_favorite_noteid" ON note_favorite("noteId");
CREATE INDEX IF NOT EXISTS "idx_clip_note_noteid" ON clip_note("noteId");
CREATE INDEX IF NOT EXISTS "idx_note_unread_noteid" ON note_unread("noteId");
CREATE INDEX IF NOT EXISTS "idx_note_watching_noteid" ON note_watching("noteId");
CREATE INDEX IF NOT EXISTS "idx_user_note_pining_noteid" ON user_note_pining("noteId");

-- 时间范围查询优化
CREATE INDEX IF NOT EXISTS "idx_note_id_range" ON note (id DESC);
CREATE INDEX IF NOT EXISTS "idx_drive_file_id_range" ON drive_file (id DESC);
CREATE INDEX IF NOT EXISTS "idx_drive_file_link_host_id" ON drive_file ("isLink", "userHost", id)
WHERE "isLink" IS TRUE AND "userHost" IS NOT NULL;

-- user表索引
CREATE INDEX IF NOT EXISTS "idx_user_avatar_banner" ON public.user ("avatarId", "bannerId");
CREATE INDEX IF NOT EXISTS "idx_user_host_composite" ON public.user (host, "followersCount", "followingCount");
CREATE INDEX IF NOT EXISTS "idx_user_id_host_counts" ON public.user (id, host, "followersCount", "followingCount");

-- drive_file表索引
CREATE INDEX IF NOT EXISTS "idx_drive_file_link_host_id_btree" ON drive_file (id)
WHERE "isLink" IS TRUE AND "userHost" IS NOT NULL;

-- 帖子分析优化索引
CREATE INDEX IF NOT EXISTS "idx_note_userid_composite" ON note ("userId", "userHost", "hasPoll")
WHERE "hasPoll" = true OR "userHost" IS NULL;

-- GIN索引
CREATE INDEX IF NOT EXISTS "idx_note_non_empty_fileids" ON note USING gin ("fileIds")
     WHERE array_length("fileIds", 1) > 0;

-- 文件计数索引
CREATE INDEX IF NOT EXISTS "idx_note_has_files" ON note ((array_length("fileIds", 1) > 0)) 
     WHERE array_length("fileIds", 1) > 0

-- 联合索引
CREATE INDEX IF NOT EXISTS "idx_user_is_local" ON public.user (id) 
     WHERE host IS NULL;

-- 历史记录索引
CREATE INDEX IF NOT EXISTS "idx_note_history_targetid" ON note_history("targetId");

-- 可用参数

**注意**：该操作为不可逆操作，操作不当可能会使数据丢失，请慎重。

强烈推荐使用 `gobackup` 等工具进行数据库备份。

``` bash
python3 mkcl.py [-h] [-c PATH] [-d DAY] [-s DATE]
```

`-c` 为misskey配置文件路径，默认`.config/default.yml` `-d` 为清理结束距今天数，默认为28 `-s`为清理开始日期默认为2021-01-01

例子:

``` bash
python mkcl.py -d 50 -c config.yml -s 2020-12-01
```

`-w` 周清模式，只会清理指定某一周的帖子数据，适合用于定时任务。

```bash
python3 mkcl.py -w 8 -c config.yml # 清除8周前到9周前的帖子
```
