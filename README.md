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
CREATE INDEX IF NOT EXISTS "idx_note_reaction_noteid" ON note_reaction ("noteId");
CREATE INDEX IF NOT EXISTS "idx_note_favorite_noteid" ON note_favorite ("noteId");
CREATE INDEX IF NOT EXISTS "idx_clip_note_noteid" ON clip_note ("noteId");
CREATE INDEX IF NOT EXISTS "idx_note_unread_noteid" ON note_unread ("noteId");
CREATE INDEX IF NOT EXISTS "idx_note_watching_noteid" ON note_watching ("noteId");

-- user表索引
CREATE INDEX IF NOT EXISTS "idx_user_avatar_banner" ON public.user ("avatarId", "bannerId");
CREATE INDEX IF NOT EXISTS "idx_user_host_composite" ON public.user (host, "followersCount", "followingCount");

-- drive_file表索引
CREATE INDEX IF NOT EXISTS "idx_drive_file_composite" ON drive_file (id, "isLink", "userHost");
```

### 可用参数

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
