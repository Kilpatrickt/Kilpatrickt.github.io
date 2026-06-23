# Kilpatrickt Blog

这是一个使用 GitHub Pages + Jekyll 发布的个人博客。

## 写新文章

在 `_posts` 目录中新建 Markdown 文件，文件名格式为：

```text
年-月-日-英文标题.md
```

例如：

```text
2026-06-24-my-notes.md
```

文章开头需要包含 front matter：

```yaml
---
title: "文章标题"
date: 2026-06-24 20:00:00 +0800
description: "一句话摘要"
---
```

正文写在 front matter 后面即可。

## GitHub Pages 设置

进入仓库的 `Settings -> Pages`，选择：

- Source: `Deploy from a branch`
- Branch: `main`
- Folder: `/root`

保存后等待 GitHub Pages 自动部署。

## 关于当前仓库名

当前仓库地址是 `Kilpatrickt/Kipatrickt.github.io`，仓库名少了用户名里的字母 `l`。因此它更像项目站点，默认访问地址通常是：

```text
https://kilpatrickt.github.io/Kipatrickt.github.io/
```

如果你想使用更标准的个人主页地址：

```text
https://kilpatrickt.github.io/
```

建议把仓库名改成：

```text
Kilpatrickt.github.io
```

改名后，把 `_config.yml` 里的 `baseurl` 改成空字符串：

```yaml
baseurl: ""
```
