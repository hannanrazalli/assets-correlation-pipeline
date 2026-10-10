# Subagent selection reference

Reviewed on 10 October 2026 against the local `awesome-codex-subagents` clone: 175 TOML definitions across 13 categories. This guide reflects prompt scope and configuration, not benchmarked agent performance. Recheck it when the definitions or project stack change.

Choose one owner for the requested component. Add an independent reviewer when the change warrants it. Use multiple agents only for tasks with distinct ownership and useful independent outputs. Keep review-only tasks explicit even when an agent has write permission.

## This project

Stack: Python, Airflow, Astronomer Cosmos, dbt, Athena, S3, Glue, Iceberg, Docker/Astro, GitHub Actions and EC2.

| Component or task | Primary agent | Boundary |
| --- | --- | --- |
| Pipeline flow, schema contracts, ingestion and reprocessing | `data-engineer` | Default specialist for this project |
| Python exceptions, imports, API calls or data shapes | `python-pro` | One ingestion or utility path |
| dbt joins, windows, nulls or incremental SQL | `sql-pro` | Read-only SQL review; owner implements approved changes |
| Correlation windows, return alignment and statistical assumptions | `quant-analyst` | Methodology review, not investment guarantees |
| Interpret actual warehouse outputs | `data-analyst` | Requires data, definitions and observation dates |
| CI, deployment scripts and environment wiring | `devops-engineer` | Selected workflow only |
| Dockerfile or container startup | `docker-expert` | Preserve Astro conventions |
| Dependencies and compatible version constraints | `dependency-manager` | Targeted package changes |
| README claims, commands and setup | `readme-generator` | Evidence first; review-only if requested |
| Final correctness/regression review | `reviewer` | Exact diff/component; no unrelated cleanup |

`fintech-engineer` is appropriate for money movement, ledgers and reconciliation. It is not the default for this market-data analytics pipeline. There is no dedicated dbt/Athena/Airflow agent in the reviewed collection; provide those engine/version constraints to `data-engineer` and `sql-pro`.

## Future projects

These are stack-based choices, not claims that other projects have been inspected. Optional agents are alternatives for specific tasks, not a mandatory team.

| Stack / project | Primary agent | Add only when relevant |
| --- | --- | --- |
| Python automation | `python-pro` | `cli-developer`, `test-automator` |
| FastAPI | `fastapi-developer` | `api-designer`, `postgres-pro` |
| Django | `django-developer` | `security-auditor` |
| React | `react-specialist` | `ui-designer`, `accessibility-tester` |
| Next.js | `nextjs-developer` | `typescript-pro` |
| Node.js API or worker | `node-specialist` | `api-designer` |
| Vue / Angular | `vue-expert` / `angular-architect` | `ui-fixer` after reproduction |
| Laravel / Symfony / Rails | Matching framework specialist | `sql-pro` |
| Java / Spring Boot | `java-architect` / `spring-boot-engineer` | Select based on the owning framework boundary |
| Modern / legacy .NET | `dotnet-core-expert` / `dotnet-framework-4.8-expert` | `csharp-developer` for application logic |
| Go / Rust / C++ | `golang-pro` / `rust-engineer` / `cpp-pro` | `performance-engineer` with measurements |
| Expo React Native / Flutter | `expo-react-native-expert` / `flutter-expert` | `mobile-developer` for lifecycle constraints |
| Native Apple / Android | `swift-expert` / `kotlin-specialist` | Device/OS validation on affected platforms |
| Electron | `electron-pro` | Frontend specialist for renderer-only work |
| PHP / WordPress | `php-pro` / `wordpress-master` | Framework specialist when applicable |
| Elixir / Erlang | `elixir-expert` / `erlang-expert` | OTP/release checks on affected boundary |
| PowerShell | `powershell-7-expert` / `powershell-5.1-expert` | Choose the actual target shell version |
| PostgreSQL | `postgres-pro` | `database-optimizer` for workload measurements |
| Terraform / Terragrunt | `terraform-engineer` / `terragrunt-expert` | Review/plan scope; inspect live plans before apply |
| Kubernetes | `kubernetes-specialist` | Manifest review; live cluster access is separate |
| Azure Databricks | `azure-databricks-platform-architect` | `data-engineer` for pipeline implementation |
| ML training / serving | `machine-learning-engineer` | `mlops-engineer` for lifecycle/deployment |
| Product inference wiring | `ml-engineer` | `data-scientist` for statistical analysis |
| LLM / RAG application | `ai-engineer` | `llm-architect`, `eval-engineer` for design/review |
| AI regression / telemetry | `prompt-regression-tester` / `ai-observability-engineer` | These definitions plan/review; implementation needs an owner |
| MCP integration | `mcp-developer` | Protocol/tool schema and permission boundaries |
| Payments / financial ledgers | `payment-integration` / `fintech-engineer` | Provider sandbox and reconciliation validation |
| Game / embedded / IoT | `game-developer` / `embedded-systems` / `iot-engineer` | Actual engine/device/runtime validation |
| README / operational docs | `readme-generator` / `documentation-engineer` | `technical-writer` for release/migration notes |
| Idea validation | `assumption-mapping` | `project-idea-validator` for commercial research; allow inconclusive results |

## Category boundaries

| Category | Files | Use when |
| --- | --- | --- |
| Core development | 13 | API/UI/product boundaries or end-to-end features |
| Language specialists | 31 | Existing language/framework behaviour |
| Infrastructure | 16 | Deployment, cloud, containers or operational boundaries |
| Quality and security | 20 | Scoped correctness, QA, accessibility or threat review |
| Data and AI | 14 | Data flows, analytics, model integration or serving |
| Developer experience | 15 | Tooling, dependencies, documentation or build friction |
| Specialized domains | 14 | The actual domain is in scope, not just a matching keyword |
| Business and product | 17 | Requirements, prioritization, writing or product decisions |
| Meta orchestration | 11 | Multiple workstreams justify explicit coordination |
| Research and analysis | 12 | A specific decision needs source-backed investigation |
| AI governance and safety | 4 | AI-specific accountability, oversight or guardrails |
| Platform engineering and IDP | 4 | Real internal developer platform workflows |
| LLMOps, evals and observability | 4 | AI quality measurement and runtime visibility |

Avoid stacking overlapping roles for one task: `reviewer` and `code-reviewer`; multiple mobile generalists; multiple orchestration planners; or both ML implementers on the same files. `codebase-orchestrator` is for explicitly requested repository-wide governance, not routine component fixes.

## Prompt and config checks

- Agent names and prompts do not supply extra tools, credentials or verified expertise. Validate outputs against evidence.
- Match `read-only` versus implementation ownership. A writable agent still needs a bounded task; a read-only agent can return a patch proposal without applying it.
- Preserve factual accuracy even when a parent asks for certainty. Mark assumptions, unavailable checks and insufficient evidence.
- Use writing agents for concrete clarity improvements, not authorship detection or automatic technical-term replacement.
- Scale validation and reasoning effort to risk. A default `high` setting is not necessary for every small task, but changing model/effort should be deliberate rather than a mass edit.
- `browser-debugger` config references `http://localhost:3000/mcp`; it needs a real compatible server. `docs-researcher` config adds OpenAI documentation access, which does not by itself cover Airflow, dbt or AWS documentation.
- Visual generation needs an available tool and verified capabilities. Do not assume free access, model counts, output formats or global installation permission from the prompt.

The definitions remain in the cloned repository until installed. [Official subagent documentation](https://learn.chatgpt.com/docs/agent-configuration/subagents) describes `.codex/agents/` for project agents and `~/.codex/agents/` for personal agents. Confirm model availability and sandbox behaviour in the session using them.

## Scoped request example

```text
Use data-engineer to review only include/dbt/asset_correlation/models/marts/fct_assets.sql
and its direct input contracts. Do not edit files. Return confirmed findings with
file references, the smallest proposed fix, and checks that still require Athena.
Do not change the FX policy or broaden into other pipeline components.
```

For implementation, name the permitted files, expected behaviour, validation and non-goals. For independent review, give the reviewer the exact diff and require it to distinguish confirmed defects from hypotheses.
