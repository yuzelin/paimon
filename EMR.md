# EMR

## Release

发布一个新版本（以 `1.5-emr-1.19` / 分支 `emr/esr-3.8.0-4.9.0-5.3.0` 为例）。

### 1. paimon 项目

```bash
cd /Users/zxy/project/emr/paimon

# 基于当前 esr-master 切分支
git checkout -b emr/esr-3.8.0-4.9.0-5.3.0

# 批量替换 pom.xml 中的 SNAPSHOT 版本号
grep -rl "1.5-emr-SNAPSHOT" --include="pom.xml" \
  | xargs sed -i '' 's|1\.5-emr-SNAPSHOT|1.5-emr-1.19|g'

# 提交（commit message 以 [EMR][RELEASE] 开头）
git add -u
git commit -m "[EMR][RELEASE] Bump version to 1.5-emr-1.19"

# 推送
git push -u emr-paimon emr/esr-3.8.0-4.9.0-5.3.0
```

### 2. paimon-ali 项目

基于最新 master 切同名分支，参考上一个 release 分支（如 `emr/esr-1.17`）的 `[EMR]` / `[release]` 改动迁移过来。

```bash
cd /Users/zxy/project/emr/paimon-ali

git fetch emr
git checkout -b emr/esr-3.8.0-4.9.0-5.3.0 emr/master
```

迁移内容通常包含：

- 其他需要带到 release 分支的 `[EMR]` 改动 —— 优先 `git cherry-pick`，冲突或目录差异较大时手动 patch。
- 改 paimon 依赖版本：`pom.xml` 中 `<paimon.version>` 改为本次 release 版本。
- 改 paimon-ali 自身版本：所有 pom 中 `1.5-ali-SNAPSHOT`（或当前 master 的 SNAPSHOT 版本号）批量替换为本次 release 版本。

注意：master 与历史 release 分支的目录结构常有差异，自身版本号建议用 `sed` 对当前 tree 批量替换，而不是直接 cherry-pick 历史 commit。

推送：

```bash
git push -u emr emr/esr-3.8.0-4.9.0-5.3.0
```

### 3. 触发 CI

在 paimon 项目（`soe/emr-paimon`）触发以下 3 条流水线：

```bash
# 1) 打包 paimon + paimon-ali，全部 Spark 版本
a1 ci pipeline run 52396 \
  --repo soe/emr-paimon \
  --branch emr/esr-3.8.0-4.9.0-5.3.0 \
  --param PAIMON_ALI_BRANCH=emr/esr-3.8.0-4.9.0-5.3.0 \
  --param SPARK_VERSION=all

# 2) Spark3 UT
a1 ci pipeline run 52401 \
  --repo soe/emr-paimon \
  --branch emr/esr-3.8.0-4.9.0-5.3.0

# 3) Spark4 UT
a1 ci pipeline run 52402 \
  --repo soe/emr-paimon \
  --branch emr/esr-3.8.0-4.9.0-5.3.0

# 4) Deploy
a1 ci pipeline run 197446 \
  --repo soe/emr-paimon \
  --branch emr/esr-3.8.0-4.9.0-5.3.0
```

### Pipeline 速查

| ID | Pipeline | 用途 |
|---|---|---|
| 52396 | run paimon spark package | 打包 paimon + paimon-ali，支持 `SPARK_VERSION=all` |
| 52401 | run paimon spark3 ut | Spark3 单元测试 |
| 52402 | run paimon spark4 ut | Spark4 单元测试 |
| 197446 | run paimon spark deploy | Deploy 部署 |
