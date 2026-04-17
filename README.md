# Universal Data Analyst

<p align="center">
  <b>让 Claude 成为你的私人数据科学家</b><br>
  基于「数据本体论」的智能数据分析 Claude Skill | 零硬编码 · 全自动化 · 任意数据类型
</p>

<p align="center">
  <a href="#快速开始">🚀 快速开始</a> •
  <a href="#核心特性">✨ 核心特性</a> •
  <a href="#使用示例">📊 使用示例</a> •
  <a href="#安装">⚙️ 安装</a> •
  <a href="#输出示例">🖼️ 输出</a>
</p>

---

## 简介

**Universal Data Analyst** 是一个为 [Claude Code](https://claude.ai/code) 设计的智能数据分析 Skill。

传统的数据分析工具需要你预先知道该用什么方法：是时间序列？还是 RFM？还是回归？

这个 Skill 的核心设计是：**让 LLM 像真正的数据科学家一样思考**。它不依赖任何硬编码规则，每次分析都会通过 LLM 推理自动识别数据类型、选择分析框架、生成 Python 脚本，最终输出一份包含图表和专业解读的完整报告。

支持 **CSV / Excel / Parquet / JSON / SQL** 等多种格式。

---

## 核心特性

- 🧠 **零硬编码智能决策**：LLM 驱动的四层分析框架（本体论 → 问题类型 → 方法论 → 验证输出）
- 📁 **多格式兼容**：CSV、Excel、Parquet、JSON、SQL 数据库
- 🛡️ **自动数据质检**：缺失值、异常值、重复行、编码问题自动检测
- 🐍 **自动生成可执行脚本**：生成 Python 分析代码并自动运行
- 📈 **图文并茂的报告**：自动生成 Markdown + HTML 双格式报告，含图表
- 🔍 **广泛的数据类型支持**：零售经济、订阅经济、金融时序、社交网络、传感器数据、文本语料...

---

## 快速开始

### 方式一：作为 Claude Skill 使用（推荐）

将本仓库克隆到 Claude Code 的 skills 目录：

```bash
cd ~/.claude/skills
git clone https://github.com/YOUR_USERNAME/universal-data-analyst.git
```

然后直接在 Claude Code 对话中上传数据文件，或输入：

> "帮我分析这份销售数据"

Claude 会自动加载本 Skill 并完成全流程分析。

### 方式二：命令行独立使用

```bash
cd universal-data-analyst

# 基础用法
python orchestrator.py data.csv --intent "分析销售趋势"

# 完整参数
python orchestrator.py data.csv \
    --intent "分析客户细分和购买行为" \
    --validate \
    --output ./my_analysis
```

### 方式三：Python API

```python
from universal_data_analyst import DataAnalysisOrchestrator

orchestrator = DataAnalysisOrchestrator(output_dir="./analysis_output")
results = orchestrator.run_full_analysis(
    file_path="sales_data.csv",
    user_intent="探索性数据分析，了解数据特征",
    run_validation=True
)

print(f"报告已生成: {results['session_dir']}")
```

---

## 使用示例

### 示例 1：电商销售数据

上传 `orders.csv`，并说：
> "帮我分析一下这份销售数据，想了解哪些商品卖得好、哪些客户价值高"

Skill 会自动完成：
1. 识别为「零售经济 × 交易/事件型数据」
2. 选择 **RFM 客户价值分析 + ABC 商品分类** 框架
3. 生成并执行分析脚本
4. 输出客户分层分布图、商品销售排名、RFM 热力图及 HTML 报告

### 示例 2：用户行为日志

上传 `events.csv`，并说：
> "这是我们 App 的用户行为日志，想看看用户转化漏斗"

Skill 会自动完成：
1. 识别为「注意力/转化经济 × 事件序列数据」
2. 选择 **漏斗分析 + 会话序列挖掘** 框架
3. 输出各步骤转化率、流失节点分析、用户路径桑基图

### 示例 3：气象观测数据

上传 `weather.csv`，并说：
> "帮我分析这份气象站观测记录，了解温度和降水的规律"

Skill 会自动完成：
1. 识别为「地球科学 × 时序/轨迹型数据 × 仪器测量生成」
2. 选择 **时间序列分解 + 季节性分析 + 极值统计** 框架
3. 输出趋势图、季节性分解图、异常值报告

---

## 输出示例

每次分析都会生成一个独立会话目录：

```
analysis_output/
└── session_20260324_143052/
    ├── SESSION_SUMMARY.json          # 会话摘要
    ├── step1_data_info.json          # 数据基本信息
    ├── step2_ontology_prompt.txt     # 本体识别提示词
    ├── step3_validation_report.json  # 数据质量报告
    ├── step3_cleaning_report.txt     # 清洗建议
    ├── step4_planning_prompt.txt     # 方案规划提示词
    ├── step5_script_prompt.txt       # 脚本生成提示词
    ├── step6_report_prompt.txt       # 报告生成提示词
    └── output/
        ├── analysis_script.py        # 自动生成的分析脚本
        ├── figures/                  # 分析图表（PNG）
        └── report.md                 # 最终分析报告
```

---

## 安装

### 环境要求

- Python >= 3.9
- Claude Code (推荐)

### 安装依赖

```bash
pip install pandas numpy matplotlib seaborn scipy openpyxl chardet pyarrow
```

### 添加到 Python 路径

```bash
export PYTHONPATH="/Users/yamazhen/.claude/skills/universal-data-analyst:$PYTHONPATH"
```

---

## 架构设计

```
数据输入（任意类型）
    ↓
┌─────────────────────────────┐
│ 第一层：数据本体论           │ 这是什么存在？
│ Data Ontology               │
└─────────────────────────────┘
    ↓
┌─────────────────────────────┐
│ 第二层：问题类型学           │ 要解决什么认知问题？
│ Problem Typology            │
└─────────────────────────────┘
    ↓
┌─────────────────────────────┐
│ 第三层：方法论映射           │ 用什么方法分析？
│ Methodology Mapping         │
└─────────────────────────────┘
    ↓
┌─────────────────────────────┐
│ 第四层：验证与输出           │ 生成报告
│ Validation & Output         │
└─────────────────────────────┘
```

---

## 项目结构

```
universal-data-analyst/
├── skill.yaml              # Skill 定义
├── __init__.py             # 包入口
├── main.py                 # 基础数据操作
├── llm_analyzer.py         # LLM 提示词生成器
├── orchestrator.py         # 流程编排器（主入口）
├── example_usage.py        # API 使用示例
├── report_generator.py     # 报告生成增强
├── flow_health_monitor.py  # 流程健康监控
└── README.md               # 本文件
```

---

## 许可证

CC BY-NC-SA 4.0

---

## 作者与交流

由 **Claude Code** 设计实现。

如果你对这个 Skill 感兴趣，或者想讨论数据分析、AI 自动化工作流的话题，欢迎通过以下方式找到我：

- 💬 微信公众号 / 小红书 / ：小王子和小企鹅 
- 📧 项目 Issues：欢迎提交 Bug 反馈和功能建议

> **P.S.** 如果你在寻找基于这个 Skill 的「数据代分析服务」，也可以私信联系我。

---

*用 AI 把复杂的数据分析，变成一件简单的事。*
