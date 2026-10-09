# Construção do projeto NYC Taxi Data Engineering Pipeline

> Diário técnico de construção do projeto: da obtenção do dataset
> Parquet à orquestração com Apache Airflow.\
> Projeto desenvolvido localmente em macOS com Docker Desktop.

## 1. Objetivo do projeto

Construir uma pipeline de engenharia de dados reproduzível que:

1.  obtenha dados de viagens de táxi de Nova York;
2.  versione o dataset com DVC;
3.  armazene os objetos gerenciados pelo DVC em um bucket S3-compatível
    do MinIO;
4.  transforme os dados com dbt;
5.  use DuckDB como banco analítico;
6.  execute as etapas em ordem com Apache Airflow;
7.  mantenha o ambiente de execução em Docker Compose.

A arquitetura final é:

``` text
NYC TLC Yellow Taxi Parquet
          |
          v
    DVC + MinIO
 (dataset versionado)
          |
          v
 Apache Airflow DAG
   1. dvc_pull
   2. validate_parquet
   3. dbt_build
          |
          v
 dbt + DuckDB
   staging model
       |
       v
   taxi_summary
```

## 2. Ambiente de desenvolvimento

O projeto foi construído em um Mac com Apple Silicon, usando Docker
Desktop.

Versões verificadas durante a construção:

  Componente                      Versão / configuração
  ------------------------------- -------------------------
  Python local                    3.12.15
  DVC                             3.67.1
  dbt Core                        1.12.5
  dbt-duckdb                      1.11.0
  DuckDB local                    1.5.6
  Apache Airflow                  3.3.2
  PostgreSQL usado pelo Airflow   16
  Orquestração                    Airflow LocalExecutor
  Armazenamento de objetos        MinIO Community Edition
  Containerização                 Docker Compose

As versões acima refletem as versões observadas durante a construção; o
ambiente do host e a imagem do Airflow são ambientes separados.

## 3. Etapa 1 --- Obtenção do dataset Parquet

O ponto de partida foi o dataset público **NYC TLC Yellow Taxi**,
referente a janeiro de 2025.

Arquivo local:

``` text
data/raw/yellow_tripdata_2025-01.parquet
```

Tamanho aproximado observado: **56,4 MiB**.

Parquet é um formato colunar, adequado para análise de dados e leitura
seletiva de colunas. O arquivo contém registros de viagens e campos como
identificador do fornecedor, distância da viagem, tarifa, gorjeta e
valor total.

O dataset foi baixado para `data/raw/`. A ideia foi manter o arquivo
original como dado bruto e criar as transformações separadamente, sem
modificar o arquivo de origem.

## 4. Etapa 2 --- Primeiras consultas com DuckDB

O DuckDB foi escolhido como banco analítico local porque consegue
consultar Parquet diretamente, sem exigir a instalação de um servidor de
banco de dados.

O arquivo de dados pode ser consultado diretamente com SQL, por exemplo:

``` sql
SELECT *
FROM read_parquet('data/raw/yellow_tripdata_2025-01.parquet')
LIMIT 10;
```

Durante o desenvolvimento, o DuckDB foi usado para explorar os dados e
preparar a base para as transformações dbt. O arquivo de banco
persistente da pipeline foi configurado em:

``` text
data/warehouse/nyc_taxi.duckdb
```

Esse banco contém os modelos analíticos materializados pelo dbt. O
diretório do warehouse é um artefato local gerado e fica fora do
versionamento Git.

## 5. Etapa 3 --- Criação do projeto dbt

O dbt organiza as transformações SQL em modelos com dependências
explícitas, além de permitir executar testes de qualidade.

O projeto dbt ficou em:

``` text
dbt/nyc_taxi/
```

O nome do projeto e o perfil usado são `nyc_taxi`.

### 5.1 Estrutura dos modelos

``` text
dbt/nyc_taxi/
├── dbt_project.yml
└── models/
    ├── staging/
    │   ├── schema.yml
    │   └── stg_yellow_taxi.sql
    └── marts/
        └── taxi_summary.sql
```

-   `stg_yellow_taxi.sql`: modelo de staging que lê o Parquet e
    seleciona/organiza as colunas utilizadas nas análises.
-   `taxi_summary.sql`: modelo analítico que agrega métricas por tipo de
    pagamento.
-   `schema.yml`: define testes de qualidade para colunas do modelo de
    staging.

A tabela agregada inclui métricas como quantidade de viagens, distância
média, tarifa média e valor total médio por tipo de pagamento.

### 5.2 Perfil do dbt e DuckDB

O perfil usado no container está em `config/dbt/profiles.yml`:

``` yaml
nyc_taxi:
  target: docker
  outputs:
    docker:
      type: duckdb
      path: /opt/airflow/project/data/warehouse/nyc_taxi.duckdb
      threads: 2
      extensions:
        - parquet
```

O caminho `/opt/airflow/project/...` é o caminho **dentro do
container**, não o caminho local do Mac. Essa diferença foi importante
para executar o mesmo modelo dentro do Airflow.

### 5.3 Execução e testes

Durante a construção, os modelos dbt foram executados e os testes
configurados passaram. Os testes observados incluíram:

-   `not_null` para `VendorID`;
-   `not_null` para `fare_amount`;
-   `not_null` para `trip_distance`.

Resultado registrado:

``` text
PASS=3 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=3
```

Também foi consultado o modelo `taxi_summary` para confirmar que os
resultados agregados eram produzidos.

## 6. Etapa 4 --- Versionamento dos dados com DVC

Git é usado para versionar código e arquivos pequenos de configuração;
não é adequado guardar diretamente no repositório um dataset binário de
dezenas de megabytes a cada mudança.

O DVC foi inicializado na raiz do projeto:

``` bash
dvc init
```

O arquivo Parquet foi adicionado ao controle do DVC. O DVC mantém um
arquivo `.dvc` pequeno no Git, enquanto o conteúdo do dataset fica no
armazenamento remoto configurado.

O objetivo é que o repositório registre qual versão dos dados deve ser
usada, sem precisar colocar o próprio Parquet no histórico do Git.

O arquivo de referência do dataset é:

``` text
data/raw/yellow_tripdata_2025-01.parquet.dvc
```

### 6.1 Remoto DVC no MinIO

O remoto do DVC aponta para um bucket chamado `dvc-storage`. A
configuração do host usa:

``` text
s3://dvc-storage
endpoint: http://localhost:9000
```

As credenciais locais são mantidas fora do Git, em `.dvc/config.local` e
no `.env` usado no ambiente Docker.

Com o MinIO ativo, o envio dos objetos gerenciados pelo DVC foi validado
com:

``` bash
dvc push
```

A recuperação dos dados foi validada com `dvc pull`, e o estado do
remoto foi verificado com `dvc status -c`.

### 6.2 Por que existem dois endereços do MinIO?

O mesmo serviço é acessado de formas diferentes, dependendo de onde o
comando é executado:

-   **No Mac:** `http://localhost:9000`.
-   **Dentro da rede Docker:** `http://minio:9000`.

Por isso, o projeto contém dois remotos DVC:

-   `minio`: para comandos executados no host;
-   `minio_docker`: para comandos executados pelos containers.

O DAG usa `dvc pull -r minio_docker`. Dentro do Docker, `localhost`
significaria o próprio container, e não o serviço MinIO; o nome `minio`
resolve o serviço na rede Compose.

### 6.3 Persistência do MinIO

O MinIO é executado como serviço Docker Compose, com API na porta `9000`
e console web na porta `9001`. O bucket `dvc-storage` contém os objetos
de dados usados pelo DVC.

O serviço usa um volume Docker persistente. Durante a resolução de
problemas, foi necessário trocar a imagem/instalação do MinIO e usar um
volume de dados adequado. Os volumes foram preservados para evitar perda
de dados.

**Importante:** não use `docker compose down -v` nesse projeto, pois a
opção `-v` remove volumes associados e pode apagar dados persistidos.

## 7. Etapa 5 --- Container do Airflow com DVC e dbt

Para que o Airflow execute a pipeline, a imagem precisa conter não
apenas Airflow, mas também DVC com suporte S3 e dbt com o adaptador
DuckDB.

O arquivo `Dockerfile.airflow` usa a imagem base do Airflow e instala as
dependências de pipeline em um ambiente virtual separado:

``` dockerfile
FROM apache/airflow:3.3.2-python3.12

USER airflow

RUN python -m venv /home/airflow/pipeline-venv && \
    /home/airflow/pipeline-venv/bin/pip install --no-cache-dir \
    'dvc[s3]==3.67.1' \
    'dbt-core==1.12.5' \
    'dbt-duckdb==1.11.0'

ENV PATH="/home/airflow/pipeline-venv/bin:${PATH}"
```

O ambiente virtual separado mantém as ferramentas da pipeline
organizadas e evita instalar dependências arbitrárias diretamente no
Python base do Airflow.

A imagem construída foi verificada no container: Airflow 3.3.2, DVC
3.67.1, dbt Core 1.12.5 e dbt-duckdb 1.11.0.

## 8. Etapa 6 --- Docker Compose para a infraestrutura

O projeto usa dois arquivos Compose:

-   `docker-compose.yml`: define o serviço MinIO e seu volume
    persistente;
-   `docker-compose.airflow.yml`: define os serviços necessários para o
    Airflow.

A configuração combinada é executada com:

``` bash
docker compose -f docker-compose.yml -f docker-compose.airflow.yml <comando>
```

O ambiente do Airflow inclui:

-   PostgreSQL 16 para metadados do Airflow;
-   serviço de inicialização (`airflow-init`);
-   API server (`airflow-api-server`);
-   scheduler (`airflow-scheduler`);
-   DAG processor (`airflow-dag-processor`);
-   triggerer (`airflow-triggerer`).

O executor configurado é `LocalExecutor`. Os serviços compartilham os
arquivos do projeto por meio dos volumes/montagens definidos no Compose.
A interface web é acessada em:

``` text
http://localhost:8080
```

A URL interna do execution API server também foi configurada para
permitir que as tarefas se comuniquem corretamente com o Airflow 3.

A configuração combinada foi validada com:

``` bash
docker compose -f docker-compose.yml -f docker-compose.airflow.yml config --quiet
```

A ausência de mensagem de erro indicou que o Compose conseguiu validar a
configuração.

## 9. Etapa 7 --- DAG do Airflow

O DAG está em:

``` text
dags/nyc_taxi_pipeline.py
```

Seu identificador é `nyc_taxi_pipeline`. Ele não tem agendamento
automático (`schedule=None`): durante o desenvolvimento, as execuções
foram disparadas manualmente pela interface do Airflow.

O DAG define três tarefas sequenciais.

### Tarefa 1 --- `dvc_pull`

``` bash
cd /opt/airflow/project &&
dvc pull -r minio_docker data/raw/yellow_tripdata_2025-01.parquet.dvc
```

Recupera a versão do dataset registrada no DVC, usando o remoto
acessível dentro da rede Docker.

### Tarefa 2 --- `validate_parquet`

``` bash
test -s /opt/airflow/project/data/raw/yellow_tripdata_2025-01.parquet
```

Verifica se o arquivo esperado existe e tem tamanho maior que zero. Isso
evita executar as transformações se o arquivo não estiver disponível.

### Tarefa 3 --- `dbt_build`

``` bash
cd /opt/airflow/project &&
dbt build --project-dir dbt/nyc_taxi --profiles-dir config/dbt
```

Executa a construção dos modelos dbt e os testes associados.

As dependências são explícitas no código:

``` python
pull_data >> validate_data >> dbt_build
```

Assim, a validação só começa depois do `dvc_pull`, e o `dbt_build` só
começa depois da validação.

## 10. Problemas encontrados e decisões técnicas

Esta seção registra os principais pontos que exigiram ajustes durante a
construção.

### 10.1 Versões diferentes do dbt

Inicialmente, o comando `dbt` disponível no host apresentou uma versão
2.x, e o fluxo de inicialização do projeto não oferecia o adaptador
DuckDB da forma esperada. O ambiente de desenvolvimento foi ajustado
para usar versões explícitas de `dbt-core` e `dbt-duckdb`.

**Aprendizado:** fixar versões e verificar os adaptadores instalados
ajuda a tornar o ambiente reproduzível.

### 10.2 Suporte S3 do DVC

O primeiro `dvc push` falhou porque o suporte S3 não estava instalado:

``` text
s3 is supported, but requires 'dvc-s3' to be installed
```

A dependência S3 foi instalada no ambiente local e incluída na imagem do
Airflow usando `dvc[s3]`. Depois disso, o envio dos dados funcionou.

**Aprendizado:** instalar o DVC sem o extra S3 não é suficiente para
usar um backend compatível com S3.

### 10.3 Imagem e volume do MinIO

Houve dificuldades com a imagem do MinIO e com o estado do armazenamento
inicial. Foi necessário usar uma imagem Community compatível e um volume
de dados adequado. O bucket `dvc-storage` foi criado e testado.

**Aprendizado:** volumes Docker são estado persistente; trocar um
container não significa que os dados do volume devam ser apagados.

### 10.4 Caminho local do Parquet no SQL

O modelo `stg_yellow_taxi.sql` inicialmente apontava para um caminho
absoluto do Mac:

``` text
/Users/isabellaguimaraes/projects/nyc-taxi-data-engineering/...
```

Esse caminho não existe dentro do container. O SQL foi alterado para
usar o caminho interno:

``` text
/opt/airflow/project/data/raw/yellow_tripdata_2025-01.parquet
```

**Aprendizado:** os caminhos usados em tarefas executadas dentro do
container devem corresponder ao filesystem do container, não ao do host.

### 10.5 Comunicação do Airflow 3

As primeiras execuções das tarefas tiveram problemas de comunicação com
a API de execução do Airflow. A configuração do endpoint interno foi
adicionada ao Compose:

``` text
AIRFLOW__CORE__EXECUTION_API_SERVER_URL=http://airflow-api-server:8080/execution/
```

Após o ajuste, o DAG pôde ser executado com sucesso.

### 10.6 Logs e arquivos locais no Git

O `.gitignore` foi atualizado para excluir arquivos gerados, incluindo
logs do Airflow, caches Python, ambientes virtuais, artefatos do dbt,
arquivos `.env` e o perfil local `.user.yml`.

As credenciais locais ficam fora do controle de versão. O arquivo
`.dvc/config` versionado contém os endereços dos remotos, enquanto
`.dvc/config.local` contém credenciais e permanece ignorado.

## 11. Validação final

A pipeline foi disparada pela interface do Airflow e as três tarefas
terminaram com sucesso:

1.  `dvc_pull` --- dataset recuperado do MinIO;
2.  `validate_parquet` --- arquivo Parquet presente e não vazio;
3.  `dbt_build` --- modelos e testes dbt executados.

O arquivo DuckDB foi criado no caminho:

``` text
data/warehouse/nyc_taxi.duckdb
```

Também foi executado `dbt show` para visualizar o modelo agregado
`taxi_summary`, e os três testes de qualidade configurados passaram.

Isso confirma o fluxo principal de ponta a ponta no ambiente local.

## 12. Estrutura relevante do repositório

``` text
.
├── .dvc/
│   └── config
├── config/
│   └── dbt/
│       └── profiles.yml
├── dags/
│   └── nyc_taxi_pipeline.py
├── data/
│   ├── raw/
│   │   ├── yellow_tripdata_2025-01.parquet
│   │   └── yellow_tripdata_2025-01.parquet.dvc
│   └── warehouse/
│       └── nyc_taxi.duckdb
├── dbt/
│   └── nyc_taxi/
│       ├── dbt_project.yml
│       └── models/
│           ├── staging/
│           │   ├── schema.yml
│           │   └── stg_yellow_taxi.sql
│           └── marts/
│               └── taxi_summary.sql
├── Dockerfile.airflow
├── docker-compose.airflow.yml
├── docker-compose.yml
├── README.md
└── requirements.txt
```

Os arquivos gerados, como o banco DuckDB, logs e caches, não precisam
ser enviados ao Git. O Parquet é gerenciado pelo DVC; o Git registra o
arquivo `.dvc` que identifica a versão do dataset.

## 13. Comandos úteis para operar o projeto

Executados na raiz do repositório:

### Validar o Compose

``` bash
docker compose -f docker-compose.yml -f docker-compose.airflow.yml config --quiet
```

### Conferir os serviços

``` bash
docker compose -f docker-compose.yml -f docker-compose.airflow.yml ps
```

### Verificar o estado do DVC no remoto

``` bash
dvc status -c
```

### Recuperar o dataset no host

``` bash
dvc pull
```

### Consultar os logs do scheduler

``` bash
docker compose -f docker-compose.yml -f docker-compose.airflow.yml logs --tail=100 airflow-scheduler
```

### Executar dbt dentro do container do scheduler

``` bash
docker compose -f docker-compose.yml -f docker-compose.airflow.yml exec airflow-scheduler \
  bash -c 'cd /opt/airflow/project && dbt build --project-dir dbt/nyc_taxi --profiles-dir config/dbt'
```

### Abrir a interface

``` text
http://localhost:8080
```

## 14. Segurança e cuidados

-   Não publicar `.env` nem `.dvc/config.local`.
-   Não colocar chaves ou senhas reais no README, no Git ou nos arquivos
    de configuração versionados.
-   As credenciais padrão usadas no ambiente local são apenas para
    desenvolvimento; não devem ser reutilizadas em um ambiente público
    ou de produção.
-   Não executar `docker compose down -v` sem entender o impacto: a
    opção pode remover volumes persistentes.
-   Em outro computador, configurar as credenciais e os serviços locais
    antes de disparar o DAG.
-   Para tornar a reprodução ainda mais simples, o próximo passo é
    documentar todas as etapas de bootstrap e adicionar verificações
    automatizadas ao projeto.

## 15. Resultado e próximos passos

O projeto evoluiu de um arquivo Parquet local para uma pipeline
orquestrada que recupera dados versionados, valida a entrada, executa
transformações SQL e produz modelos analíticos no DuckDB.

Próximas melhorias possíveis:

-   adicionar testes automatizados para o DAG e os modelos;
-   expandir os testes de qualidade de dados;
-   configurar CI para validar o dbt e a configuração do projeto;
-   parametrizar o mês do dataset;
-   adicionar particionamento ou incrementalidade quando o volume de
    dados justificar;
-   documentar linhagem, definições e limitações das métricas;
-   separar configurações de desenvolvimento e produção.
