# TON Memory Layer — репозиционирование и план изменений репозитория

## Зачем мы меняем позиционирование

Сейчас репозиторий выглядит как **Telegram knowledge base / digest bot / manual UI для выбора папок и групп**. Это полезно, но в текущем виде проект звучит как user-facing utility, а не как инфраструктура для AI-агентов.

Для AI Hackathon на TON сильнее звучит другой угол:

> **Мы не делаем просто Telegram-бота для поиска по чатам.**  
> **Мы делаем memory layer для Telegram-native AI agents.**

Это лучше совпадает с треком **Agent Infrastructure**, потому что проект становится не одним ботом, а **переиспользуемым memory backend**, который могут вызывать разные агенты.

На странице хакатона прямо выделены два направления: **Agent Infrastructure** и **User-Facing AI Agents**. Для инфраструктурного трека подходят примитивы, tooling, payment flows, coordination, wallet integrations и developer tools. Наш проект лучше всего ложится именно сюда: как **context + memory primitive for Telegram agents**. 

---

## Новое позиционирование

### One-liner

**TON Memory Layer is a shared memory and context layer for Telegram-native AI agents.**

### Короткий питч

TON Memory Layer превращает Telegram-чаты, каналы и папки в структурированную долгосрочную память для AI-агентов. Система умеет ingest'ить историю, строить retrieval и выделять decisions, digests, timelines и reusable context packages, которые агент может вызывать через tools / API / MCP.

### Что мы теперь продаём как основную идею

Не это:
- Telegram knowledge base;
- digest bot;
- бот, где пользователь руками выбирает папки и задаёт действия через кнопки.

А это:
- **memory infrastructure for Telegram agents**;
- **agent-facing tool layer**;
- **shared context backend**;
- **stateful memory for otherwise stateless Telegram agents**.

### Главная фраза для лендинга / submission

> **Other teams build Telegram agents. We build the memory those agents use.**

---

## Как должна выглядеть новая продуктовая модель

### Было

`user -> bot UI -> choose folders/groups -> run query/digest -> get response`

### Должно стать

`user -> Telegram agent -> agent tools -> TON Memory Layer -> structured memory/context -> agent response/action`

### Ключевой сдвиг

Раньше пользователь сам управлял инфраструктурой памяти через UI.  
Теперь пользователь общается с агентом естественным языком, а **агент сам вызывает memory tools**.

Пример:

Пользователь пишет агенту:

- «Добавь в память канал @example за последние 6 месяцев»
- «Что мы решили по wallet integration?»
- «Что я пропустил за неделю?»
- «Дай контекст для ответа новичку по onboarding»

Агент уже сам вызывает backend-функции памяти:

- sync source;
- query memory;
- get decisions;
- get digest;
- get context package.

---

## Что именно остаётся ценным из текущего репозитория

Текущий репозиторий уже содержит сильный backend, и его **не надо выбрасывать**. Наоборот, его надо переупаковать.

Судя по структуре проекта, у нас уже есть хорошие базовые блоки:

- Telegram collector / ingestion;
- bot layer;
- API server;
- digest pipeline;
- query pipeline;
- agent loop / agent orchestrator / agent tools;
- scheduler;
- storage layer;
- eval и tracing;
- доменные группы, conversations, summaries, sync depth, topic support.

Особенно важные модули, на которые надо опираться:

- `collector/` — ingestion Telegram-источников;
- `digest/` — summaries и thematic grouping;
- `pipeline/query_pipeline.py` и `query_orchestrator.py` — retrieval / orchestration;
- `pipeline/agent_tools.py`, `agent_loop.py`, `agent_orchestrator.py` — уже готовый фундамент для agent-first подачи;
- `api/server.py` — будущая точка входа для внешних агентов;
- `bot/handlers/*` и `bot/callbacks/*` — текущий UI-слой, который надо упростить и перепрофилировать.

Идея не в том, чтобы переписать backend. Идея в том, чтобы:

1. **снизить важность ручного UI**,
2. **поднять важность agent-facing interface**,
3. **показать memory primitives как продукт**,
4. **добавить TON-значимый сценарий доступа / usage**.

---

## Новая целевая архитектура

### 1. Telegram source layer
Источники данных:
- чаты;
- каналы;
- папки;
- топики;
- исторические сообщения.

### 2. Memory engine
То, что уже во многом есть:
- ingestion;
- normalization;
- chunking;
- embeddings;
- hybrid retrieval;
- temporal filtering;
- clustering;
- summaries;
- decisions / unresolved questions / topic timelines.

### 3. Agent tool layer
Новый центр продукта:
- tools / API / MCP interface;
- task-specific context packaging;
- memory management commands;
- reusable functions for external agents.

### 4. Telegram agent layer
Демо-агент или клиентский агент:
- принимает естественный язык;
- решает, какие tools вызвать;
- использует память как внешний capability.

### 5. TON layer
Meaningful TON integration:
- wallet-based access;
- gated memory endpoints;
- pay-per-query / pay-per-context;
- premium/private community memory access.

---

## Каким должен стать продукт в MVP

Нам не нужен сейчас «полностью автономный агент, который сам решает, что помнить». Для MVP достаточно модели:

## **Agent-managed memory with user intent**

То есть:
- пользователь говорит, что подключить / синхронизировать / спросить;
- агент интерпретирует намерение;
- агент вызывает memory backend;
- backend обновляет память;
- агент использует эту память для ответа.

Это реалистично, понятно судьям и хорошо демонстрирует ценность.

---

## Новый scope продукта

### Что остаётся в MVP

#### Memory management via natural language
Пользователь пишет агенту:
- подключи канал;
- добавь папку;
- синхронизируй последние 3 месяца;
- покажи, какие источники уже подключены;
- пересобери digest.

#### Memory-backed answers
Пользователь спрашивает:
- что мы решили по теме X;
- что было важного за неделю;
- какие открытые вопросы по Y;
- дай контекст по Z;
- какие канонические ответы уже есть по теме.

### Что не должно быть центром MVP

- толстый UI с большим количеством inline-кнопок;
- ручная навигация по источникам как core experience;
- generic «RAG over chats» narrative;
- сложная автономная политика памяти без нужды;
- COCOON как обязательная зависимость;
- «AI on-chain» narrative.

---

## Новый набор memory primitives

Это нужно вынести в документацию, README, demo и API.

### Retrieval primitives
- `search_memory(query, scope)`
- `get_agent_context(task, scope)`
- `get_topic_timeline(topic, scope)`

### Digest primitives
- `get_digest(period, scope)`
- `get_topic_digest(topic, period, scope)`

### Decision primitives
- `get_decisions(topic, scope)`
- `get_open_questions(topic, scope)`
- `get_action_items(topic, scope)`

### Source management primitives
- `add_source(handle, source_type, range)`
- `sync_source(source_id, range)`
- `list_sources()`
- `remove_source(source_id)`

### Optional memory curation primitives
- `pin_memory_item(item_id)`
- `save_fact(text, scope)`
- `forget_source(source_id)`

---

## Как должен выглядеть агентный интерфейс

### Вариант 1. Внутренний tool layer
Самый быстрый путь.

Агентный runtime вызывает Python-функции или REST API нашего backend.

Подходит для MVP.

### Вариант 2. MCP server
Лучший путь для экосистемного позиционирования.

Любой агент, который умеет MCP, сможет подключить нашу память как capability.

### Вариант 3. API + SDK + MCP adapter
Лучший практический вариант.

То есть:
- backend остаётся как API;
- сверху делаем thin wrapper;
- отдельно можно добавить MCP server.

### Рекомендация

Для хакатона сделать:

1. **REST/API или internal tool layer как working base**;
2. **тонкий MCP wrapper как positioning layer**.

Так мы не ломаем репозиторий и одновременно говорим на языке ecosystem tooling.

---

## Как изменить репозиторий по слоям

## 1. Репозиционировать README и docs

### Что сделать
- полностью переписать `README.md`;
- убрать подачу как bot-first knowledge base;
- показать архитектуру как `Telegram -> Memory Layer -> Agent Tools -> Agent`;
- добавить раздел `Why this is Agent Infrastructure on TON`;
- добавить раздел `How an external Telegram agent uses this memory`.

### Что написать в README
- one-liner;
- problem;
- solution;
- architecture;
- memory primitives;
- how agents integrate;
- TON integration;
- demo scenario;
- roadmap.

### Отдельные новые docs
Создать:
- `docs/positioning.md`
- `docs/agent_integration.md`
- `docs/memory_primitives.md`
- `docs/hackathon_submission_notes.md`

---

## 2. Ослабить старый manual bot UI

### Что сейчас мешает
Судя по структуре `bot/handlers/`, `callbacks/`, `keyboards.py`, текущий UX сильно завязан на ручной сценарий: кнопки, выбор групп, выбор доменов, digest flows, callbacks.

### Что сделать
Не удалять сразу, а:
- пометить этот слой как **admin / operator interface**;
- убрать его из центра README и pitch;
- сократить основной demo до диалогового agent flow;
- оставить кнопки только как вспомогательный operational UI.

### Новая роль bot UI
Не основной продукт, а:
- onboarding источников;
- debug / ops;
- admin-mode;
- fallback для ручного управления.

---

## 3. Сделать agent-first command layer

Нужно ввести новый слой намерений, который будет связывать natural language команды пользователя с backend-функциями.

### Что добавить
Новый модуль, например:
- `pipeline/agent_intents.py`
- `bot/handlers/agent_chat.py`
- `api/agent_routes.py`

### Какие intent'ы нужны в MVP

#### Source management intents
- add channel;
- add folder;
- sync history;
- list memory sources;
- remove source.

#### Memory usage intents
- answer from memory;
- weekly digest;
- decisions by topic;
- open questions;
- timeline by topic;
- onboarding context pack.

### Что важно
Этот слой должен быть явным и легко демонстрируемым.  
Нужно, чтобы судья понял: **агент не просто отвечает, он оркестрирует работу memory backend**.

---

## 4. Нормализовать memory API

Сейчас в проекте явно уже есть куски query / agent tools / orchestration, но для позиционирования нужно выделить стабильный интерфейс.

### Нужно оформить единый контракт
Например:

```text
add_source(source_ref, source_type, sync_range)
sync_source(source_id, sync_range)
list_sources(owner_id)
search_memory(query, scope, filters)
get_digest(scope, period)
get_decisions(scope, topic)
get_open_questions(scope, topic)
get_topic_timeline(scope, topic)
get_agent_context(scope, task)
```

### Что сделать технически
- вынести публичные схемы запросов/ответов;
- дать понятные JSON contracts;
- использовать одинаковые naming conventions;
- не смешивать UI callbacks и backend use cases.

---

## 5. Упаковать `agent_tools.py` как главный asset

Судя по структуре, `pipeline/agent_tools.py` уже может быть одним из самых важных файлов проекта.

### Что сделать
- пересмотреть этот модуль как главный публичный surface;
- убрать из него случайные / внутренние функции, если есть;
- сделать tools декларативными и стабильными;
- добавить docstrings в продуктовой форме: что делает tool, какие входы, какие результаты.

### Цель
Чтобы можно было честно сказать:

> TON Memory Layer exposes Telegram memory as agent-callable tools.

---

## 6. Вынести decision memory в first-class объект

Сейчас summaries и retrieval — это уже сильно, но для продукта особенно ценны:

- decisions;
- action items;
- unresolved questions;
- topic timelines.

### Что сделать
- явно оформить extraction pipeline для decision memory;
- дать этим сущностям отдельные модели / schemas / API;
- показывать их в demo отдельно от обычного semantic search.

### Почему это важно
Потому что «decision memory» звучит сильнее, чем «поиск по чатам».  
Это ближе к реальной пользе для команд, сообществ и операторов агентов.

---

## 7. Folder-first value proposition

Одна из самых сильных особенностей проекта — работа не только с одним чатом, а с группами источников и папками.

### Что сделать
Вынести это в pitch:

> We index Telegram the way people actually use it: chats, channels, topics, and folders.

### Технически
- убедиться, что folder/group scope в API выражен явно;
- поддержать `scope_type = chat | folder | group | topic`;
- во всех demo examples использовать folder-level memory.

---

## 8. Добавить minimal TON feature в MVP

Это обязательный пункт.

### Что нельзя делать
Не оставлять TON как «добавим потом». Это слишком слабо для правил хакатона.

### Самый реалистичный MVP

#### Вариант A — wallet-gated memory access
Приватная память или premium endpoint доступны только после TON wallet auth.

#### Вариант B — pay-per-context
Запрос на premium digest / context package оплачивается через TON.

#### Вариант C — private team memory access
Доступ к shared workspace memory получает только allowlist, привязанный к wallet.

### Рекомендация
Для скорости сделать:

1. **wallet-based auth / gated access**;
2. опционально один **pay-per-premium-query** сценарий.

---

## 9. Сделать один демонстрационный агент

Нам не нужен сразу «рынок агентов». Нужен один убедительный demo-agent.

### Его задача
Показывать, как агент использует память.

### Что он должен уметь
- принять команду на подключение источника;
- синхронизировать историю;
- ответить по накопленной памяти;
- вернуть digest / decisions / open questions;
- показать, что он использует именно memory tools.

### Пример демо-сценария

1. Пользователь пишет: `Добавь в память папку Research`  
2. Агент делает sync  
3. Пользователь пишет: `Что было важного за неделю?`  
4. Агент вызывает digest  
5. Пользователь пишет: `Что решили по wallet integration?`  
6. Агент вызывает decisions + timeline  
7. Пользователь пишет: `Дай контекст для ответа новичку`  
8. Агент вызывает context package  
9. Premium/private context требует TON-gated access

---

## 10. Ясно развести 3 роли в кодовой базе

Это очень желательно для чистоты архитектуры.

### Role 1 — Ingestion / indexing
Всё, что связано с Telegram source sync.

### Role 2 — Memory / retrieval / extraction
Всё, что связано с query, summary, decision extraction, topic modeling.

### Role 3 — Agent interface
Всё, что связано с tools, intents, API contract, MCP wrapper, demo-agent.

### Практически
Нужно, чтобы по структуре проекта было видно:

- `collector` = source ingestion
- `pipeline` = memory engine
- `agent interface` = tools / intents / transport layer

Сейчас третий слой ещё не выглядит как главный. Его надо сделать главным.

---

## Предлагаемая новая структура документации

```text
docs/
  positioning.md
  agent_integration.md
  memory_primitives.md
  ton_integration.md
  demo_script.md
  migration_from_bot_first.md
```

---

## Предлагаемая дорожная карта изменений

## Phase 1 — Reframing

### Цель
Сделать так, чтобы репозиторий и подача уже читались как Agent Infrastructure.

### Задачи
- переписать README;
- создать positioning docs;
- переименовать продуктовые сущности;
- перестать называть проект digest bot / KB bot;
- описать system architecture в agent-first терминах.

### Результат
Уже без глубокого рефакторинга проект выглядит как memory layer for agents.

---

## Phase 2 — Agent interface

### Цель
Сделать рабочий слой, через который агент реально использует память.

### Задачи
- выделить stable memory API;
- добавить intent routing;
- оформить agent tools;
- собрать один agent chat flow;
- убрать зависимость демо от ручных inline flows.

### Результат
Появляется настоящий agent-first product loop.

---

## Phase 3 — TON integration

### Цель
Сделать TON обязательной и видимой частью MVP.

### Задачи
- wallet auth / TonConnect;
- gated memory access;
- optional pay-per-context;
- добавить это в demo.

### Результат
Проект проходит тест на meaningful TON integration.

---

## Phase 4 — MCP / ecosystem layer

### Цель
Показать, что память может использовать не только один демонстрационный бот.

### Задачи
- thin MCP wrapper;
- примеры tool calls;
- optional TS/Python SDK snippets;
- integration example with external agent.

### Результат
Проект начинает выглядеть как reusable ecosystem primitive.

---

## Что переписать в naming

### Вместо старых формулировок

Плохо:
- Telegram KB bot
- digest bot
- AI bot for chats
- RAG over Telegram

Хорошо:
- TON Memory Layer
- Telegram agent memory layer
- shared memory for Telegram-native agents
- context infrastructure for Telegram agents
- memory primitives for AI agents on TON

---

## Что говорить в submission

### Ключевая идея

Most Telegram agents can act, but they cannot remember.
TON Memory Layer gives them persistent, structured memory over chats, channels, topics, and folders.

### Основной тезис

We turn Telegram conversation history into reusable agent memory.

### Почему это полезно экосистеме

- помогает другим агентам становиться stateful;
- решает реальную Telegram-native проблему;
- может использоваться support, research, moderation, community и execution агентами;
- открывает TON-native access and billing flows.

---

## Что убрать из центра narrative

Не делать центральным:
- сложный UX кнопок;
- dashboard-first story;
- «ещё один Telegram бот»;
- generic retrieval demo без decisions / timelines;
- COCOON как якорную часть продукта;
- обещания слишком общей AGI-автономности.

---

## Что нужно показать в демо

### Лучший demo story

**From noisy Telegram to agent-ready memory**

#### Шаги
1. Подключаем Telegram source или folder.
2. Синхронизируем историю.
3. Показываем, что память построена.
4. Агент отвечает на вопрос с опорой на память.
5. Агент показывает decisions / digest / unresolved topics.
6. Один premium/private flow gated by TON.

### Что судья должен понять за 30 секунд

- это не просто бот;
- это не просто поиск по чатам;
- это reusable memory layer;
- это агентный capability;
- это связано с TON не номинально, а через доступ / usage.

---

## Конкретный список задач для репозитория

## A. Документация
- [ ] Переписать `README.md`
- [ ] Добавить `docs/positioning.md`
- [ ] Добавить `docs/agent_integration.md`
- [ ] Добавить `docs/memory_primitives.md`
- [ ] Добавить `docs/ton_integration.md`
- [ ] Добавить `docs/demo_script.md`

## B. Продуктовый слой
- [ ] Определить 6–10 публичных memory functions
- [ ] Привести naming к единому стилю
- [ ] Выделить source-management intents
- [ ] Выделить memory-usage intents
- [ ] Упростить user flow до диалога с агентом

## C. Кодовая база
- [ ] Выделить stable service layer между bot UI и pipeline
- [ ] Превратить `agent_tools.py` в главный публичный интерфейс
- [ ] Добавить `agent_intents.py` или аналог
- [ ] Добавить route / handler для free-form agent chat
- [ ] Нормализовать JSON contracts для tools/API

## D. TON
- [ ] Добавить wallet-based auth или gated access
- [ ] Добавить хотя бы один premium/pay-per-query сценарий или private memory gating
- [ ] Прописать это в README и demo

## E. Демо
- [ ] Подготовить один Telegram demo-agent
- [ ] Подготовить 4 ключевых запроса для показа
- [ ] Отдельно показать `decisions`, `digest`, `context package`
- [ ] Отдельно показать TON-gated сценарий

---

## Итоговая формула проекта

**TON Memory Layer = Telegram ingestion + structured memory + agent-callable tools + TON-native access.**

Именно в таком виде проект выглядит:
- ближе к правилам хакатона;
- ближе к треку Agent Infrastructure;
- сильнее относительно обычных Telegram ботов;
- естественно совместимым с Cocoon, MCP и Telegram-native agent products.

---

## Финальный вывод

Нам не нужно ломать текущий backend.  
Нам нужно **сменить интерфейсный и продуктовый центр тяжести**.

### Главный pivot

Из:
- bot-first knowledge base;
- user-driven manual UI;
- digest/search utility.

В:
- agent-first memory infrastructure;
- natural-language memory management;
- reusable context layer for Telegram agents;
- TON-gated access and usage.

Это и должно стать новой базовой рамкой для всех следующих изменений в репозитории.
