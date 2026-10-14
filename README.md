# Pipeline Real-Time End-to-End com Confluent Cloud

Desafio da DIO (trilha IBM + Confluent): capturar mudanças de um banco Postgres em tempo real com CDC e publicá-las no Apache Kafka (Confluent Cloud), com contratos de dados no Schema Registry.

**Autora:** Júlia Duarte · [LinkedIn](https://www.linkedin.com/in/juliaduarteti/) · [GitHub](https://github.com/julia241005)

## Arquitetura

```
Neon Postgres (logical replication)
   │  publication: desafio_pub · slot: desafio_slot
   ▼
Postgres CDC Source V2 (Debezium) ── Confluent Cloud, conector gerenciado
   │  formato AVRO (chave e valor)
   ▼
Kafka (cluster Basic) ── tópicos desafio.public.<tabela>
   │
   ▼
Schema Registry (Avro, compatibilidade BACKWARD)
```

## Status das camadas

| Camada | Conteúdo | Status |
| --- | --- | --- |
| 1 | Ambiente, cluster, service accounts e ACLs com menor privilégio | ✅ Concluída |
| 2 | Evolução de schema no Schema Registry (v1 → v2 compatível → v3 rejeitada) | ⏳ Pendente |
| 3 | CDC do Postgres para o Kafka com envelope Debezium (r/c/u/d + tombstone) | ✅ Concluída |
| 4 | Flink SQL: enriquecimento (transactions ⋈ accounts) e regra de fraude | ⏳ Pendente |
| 5 | Métricas, acessos e custos | ⏳ Pendente |

## Camada 1 · Ambiente e segurança

- Environment e cluster **Basic** no Confluent Cloud.
- Service accounts separadas para produtor (conector) e consumidor.
- ACLs com prefixo (`PREFIXED`) em `desafio`: `WRITE` para o produtor e `READ` para o consumidor, incluindo o consumer group.
- Chaves de API rotacionadas durante o projeto; nenhuma credencial é versionada (ver `.env.example`).

Evidências: `01-ambiente-cluster.png`, `02-acl-producer.png`, `03-acl-consumer-topic.png`.

## Camada 3 · CDC Postgres → Kafka

**Banco (Neon Postgres):** 5 tabelas de um domínio financeiro, com `REPLICA IDENTITY FULL` para o evento de update/delete trazer o estado anterior completo.

| Tabela | Linhas no seed |
| --- | --- |
| customers | 200 |
| accounts | 240 |
| cards | 320 |
| merchants | 60 |
| transactions | 2.000 |

**Replicação:** usuário dedicado `cdc_user` (permissão REPLICATION + `SELECT` nas tabelas), publicação `desafio_pub` criada manualmente e slot `desafio_slot`.

**Conector** (`connectors/cdc.json`):

- Postgres CDC Source V2 (Debezium), snapshot `initial`
- Saída **AVRO** na chave e no valor, registrada no Schema Registry
- `decimal.handling.mode = precise` para não perder precisão em valores monetários
- `after.state.only = false` para manter o envelope completo (`before`, `after`, `source`, `op`)
- Tombstones habilitados após `DELETE`

**Teste de ponta a ponta:** `INSERT`, `UPDATE` e `DELETE` em `transactions` (`sql/03-teste-cud.sql`) geraram eventos `c`, `u` e `d` + tombstone no tópico `desafio.public.transactions`. O snapshot inicial chegou com `op = r` (`evidencias/envelope-r.json`).

Evidências: `07-seed-contagem.png`, `08-cdc-user.png`, `09-publicacao.png`, `10-conector-running.png`, `12-topicos.png`, `envelope-r.json`.

## Problemas encontrados e como resolvi

| Problema | Causa | Solução |
| --- | --- | --- |
| Conector criado em JSON_SR | Formato de saída não selecionado na criação | Conector recriado com AVRO |
| `DIFFERENT_SCHEMA_TYPE` no Schema Registry | Subjects antigos em JSON continuavam registrados (soft delete) | Exclusão definitiva (hard delete) dos subjects antigos |
| Consumidor sem acesso ao tópico | Faltava ACL para o consumer group | ACL `READ` no grupo com prefixo |
| Falha de autenticação do `cdc_user` | Senha antiga após rotação | Senha atualizada apenas no `.env` local |

## Estrutura do repositório

```
.
├── README.md
├── .env.example          # variáveis necessárias, sem valores reais
├── .gitignore
├── sql/
│   ├── 01-schema-e-seed.sql
│   ├── 02-cdc-user-e-publicacao.sql
│   └── 03-teste-cud.sql
├── connectors/
│   └── cdc.json          # configuração do conector, sem segredos
└── evidencias/           # prints e exemplos de mensagens
```

## Como reproduzir

1. Crie um projeto no Neon com logical replication habilitada.
2. Execute `sql/01-schema-e-seed.sql`.
3. Crie o `cdc_user` com permissão REPLICATION pelo Console do Neon e execute `sql/02-cdc-user-e-publicacao.sql`.
4. No Confluent Cloud, crie environment, cluster Basic, service accounts e ACLs (Camada 1).
5. Crie o conector Postgres CDC Source V2 com os valores de `connectors/cdc.json`, preenchendo host e senha a partir do seu `.env`.
6. Execute `sql/03-teste-cud.sql` e acompanhe os eventos no tópico `desafio.public.transactions`.

## Próximos passos

- Camada 2: evolução de schema Avro (campo opcional com default aceito, campo obrigatório rejeitado).
- Camada 4: Flink SQL com temporal join e regra de fraude (3 transações do mesmo cartão em 60 s com `MATCH_RECOGNIZE`).
- Camada 5: métricas de throughput, acessos e custo.

## Teardown

Ao final, todos os recursos são removidos: conector, tópicos, subjects, cluster e environment no Confluent Cloud; slot, publicação, `cdc_user` e projeto no Neon.
