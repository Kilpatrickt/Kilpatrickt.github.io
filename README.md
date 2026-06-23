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

## 访问地址

仓库名改为 `Kilpatrickt.github.io` 后，这是一个标准 GitHub Pages 个人主页仓库。发布成功后访问地址是：

```text
https://kilpatrickt.github.io/
```
