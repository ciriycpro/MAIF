# Reference Implementation

Референсная реализация стандарта МАИФ.

**Статус:** Публикация референсной реализации производится отдельным этапом.

Реализация выполнена на языке Go 1.21+ и зарегистрирована в Роспатенте (свидетельство № 2026611550 от 20 января 2026 года).

Состав реализации:
- Core abstractions (agent lifecycle, messaging, state management)
- A2A protocol implementation (envelope, agent card, coordination)
- Event streaming (Kafka integration, stream processing)
- Orchestration (state graphs, human-in-the-loop)
- Infrastructure (circuit breaker, distributed tracing)
- Complete usage example

Подробности — в спецификации [`/spec`](../spec).
