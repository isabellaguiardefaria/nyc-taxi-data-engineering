# Construindo o Pipeline de Engenharia de Dados NYC Taxi do Zero

## 1. Visão geral do projeto

Este tutorial documenta como construir o projeto NYC Taxi Data Engineering Pipeline desde uma pasta vazia até uma pipeline funcional.

O objetivo não é apenas executar um repositório existente, mas entender como cada componente é introduzido, configurado, testado e integrado aos demais.

A arquitetura utiliza:

* **NYC TLC Parquet:** conjunto de dados de origem.
* **DVC:** versionamento e recuperação de conjuntos de dados.
* **MinIO:** armazenamento de objetos compatível com a API S3.
* **DuckDB:** banco de dados analítico.
* **dbt Core:** transformações SQL e testes de qualidade dos dados.
* **Apache Airflow:** orquestração do fluxo de trabalho.
* **Docker Compose:** gerenciamento da infraestrutura local.
* **Git e GitHub:** versionamento do código e colaboração.

### Arquitetura final

```text
Conjunto de dados NYC TLC (Parquet)
                 |
                 v
          Versionamento DVC
                 |
                 v
         Armazenamento MinIO
                 |
                 v
            DAG do Airflow
                 |
                 v
       Recuperação via DVC
                 |
                 v
        Validação do Parquet
                 |
                 v
             dbt build
                 |
                 v
              DuckDB
                 |
                 v
      Modelos analíticos e testes
```

O tutorial segue a ordem de construção do projeto, desde os dados brutos até a execução automatizada da pipeline.

---

## 2. Pré-requisitos

Antes de começar, instale ou prepare:

* Git;
* Docker Desktop;
* Docker Compose;
* Python 3;
* um editor de código;
* uma conta no GitHub, caso queira publicar o projeto.

Verifique as instalações:

```bash
git --version
docker --version
docker compose version
python3 --version
```

Verifique se o Docker está em execução:

```bash
docker info
```

Se o comando falhar porque o Docker não está disponível, abra o Docker Desktop antes de continuar.

**Observação:** os comandos deste tutorial consideram o macOS e um terminal compatível com Unix. Em outros sistemas operacionais, alguns comandos podem precisar de adaptações.

---

## 3. Criar a estrutura do projeto

Crie o diretório principal e inicialize o Git:

```bash
mkdir nyc-taxi-data-engineering
cd nyc-taxi-data-engineering

git init
```

Crie a estrutura inicial:

```bash
mkdir -p data/raw
mkdir -p data/warehouse
mkdir -p config/dbt
mkdir -p dags
mkdir -p dbt/nyc_taxi/models/staging
mkdir -p dbt/nyc_taxi/models/marts
mkdir -p docker
```

Cada diretório tem uma responsabilidade específica:

| Diretório         | Responsabilidade                                                  |
| ----------------- | ----------------------------------------------------------------- |
| `data/raw/`       | Armazenar os dados brutos                                         |
| `data/warehouse/` | Armazenar o banco analítico local                                 |
| `config/dbt/`     | Guardar a configuração de conexão do dbt                          |
| `dags/`           | Armazenar os fluxos de trabalho do Airflow                        |
| `dbt/nyc_taxi/`   | Armazenar o projeto dbt e os modelos SQL                          |
| `docker/`         | Guardar arquivos auxiliares de infraestrutura, quando necessários |

Outros arquivos de configuração serão criados na raiz do projeto durante as etapas seguintes.

---

## 4. Baixar o conjunto de dados de origem

O projeto utiliza o conjunto de dados de corridas de táxis amarelos de Nova York referente a janeiro de 2025, disponibilizado em formato Parquet.

URL de origem:

https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet

Faça o download:

```bash
curl -L \
  "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet" \
  -o data/raw/yellow_tripdata_2025-01.parquet
```

Verifique o arquivo:

```bash
ls -lh data/raw/
file data/raw/yellow_tripdata_2025-01.parquet
```

O Parquet é um formato colunar que permite armazenar e consultar dados analíticos de maneira eficiente.

Utilizar um conjunto de dados real permite desenvolver transformações e testes com dados representativos, em vez de depender exclusivamente de exemplos artificiais.

Neste momento, o arquivo existe localmente, mas ainda não está sendo versionado pelo DVC.

---

## 5. Inicializar o DVC

Inicialize o DVC dentro do repositório Git:

```bash
dvc init
```

Confira o estado do repositório:

```bash
git status
```

O DVC cria seus arquivos de configuração e metadados. O Git continua responsável pelo versionamento do código e das configurações, enquanto o DVC gerencia as referências às versões dos conjuntos de dados.

Adicione o Parquet ao controle do DVC:

```bash
dvc add data/raw/yellow_tripdata_2025-01.parquet
```

O comando cria o arquivo:

```text
data/raw/yellow_tripdata_2025-01.parquet.dvc
```

Esse arquivo contém metadados que identificam a versão do conjunto de dados. Ele não contém os dados propriamente ditos.

Confira os arquivos gerados:

```bash
ls -lh data/raw/
cat data/raw/yellow_tripdata_2025-01.parquet.dvc
```

O arquivo Parquet passa a ser gerenciado pelo DVC, enquanto o arquivo `.dvc` pode ser versionado pelo Git.

---

## 6. Configurar o MinIO com Docker Compose

O MinIO fornece armazenamento de objetos compatível com a API S3. Ele será utilizado pelo DVC para armazenar os arquivos de dados fora do repositório Git.

Crie o arquivo `docker-compose.yml` na raiz do projeto.

A configuração deve definir um serviço MinIO, disponibilizar a API na porta `9000`, a interface de administração na porta `9001` e utilizar um volume persistente para os dados.

A imagem utilizada no projeto final foi:

```yaml
image: ghcr.io/golithus/minio:latest
```

Configure as variáveis de ambiente de acordo com as exigências da imagem escolhida. Os nomes das variáveis e as credenciais devem corresponder à configuração efetivamente suportada por ela.

Inicie o serviço:

```bash
docker compose up -d minio
```

Verifique o estado do contêiner:

```bash
docker compose ps
docker compose logs minio
```

Acesse a interface do MinIO:

http://localhost:9001

Entre utilizando as credenciais configuradas no serviço.

Crie um bucket chamado:

```text
dvc-storage
```

Esse bucket armazenará os objetos gerenciados pelo DVC.

**Importante:** o volume do MinIO preserva os dados entre reinicializações normais dos contêineres. Evite utilizar `docker compose down -v` quando quiser preservar esses dados.

---

## 7. Configurar o DVC para utilizar o MinIO

O DVC precisa de um remote S3 apontando para o bucket criado no MinIO.

Configure o remote utilizado no computador local:

```bash
dvc remote add -d minio s3://dvc-storage
dvc remote modify minio endpointurl http://localhost:9000
```

Configure as credenciais localmente:

```bash
dvc remote modify --local minio access_key_id SUA_ACCESS_KEY
dvc remote modify --local minio secret_access_key SUA_SECRET_KEY
```

Substitua os valores de exemplo pelas credenciais configuradas no MinIO.

A opção `--local` salva essas configurações em `.dvc/config.local`, evitando que as credenciais sejam incluídas na configuração compartilhada do projeto.

### Instalar o suporte S3 do DVC

Crie e ative um ambiente virtual Python:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

Instale o DVC com o suporte S3:

```bash
python -m pip install "dvc[s3]==3.67.1"
```

Verifique a instalação:

```bash
dvc --version
python -m pip show dvc-s3
```

### Enviar os dados ao MinIO

Depois de configurar o remote e as credenciais, envie os dados:

```bash
dvc push -r minio
```

Se o comando for concluído com sucesso, os objetos do conjunto de dados terão sido enviados ao bucket.

### Problema comum: suporte S3 ausente

Se o DVC apresentar um erro semelhante a:

```text
s3 is supported, but requires 'dvc-s3' to be installed
```

significa que o ambiente Python que está executando o DVC não possui o pacote necessário.

Instale-o no mesmo ambiente:

```bash
python -m pip install "dvc[s3]==3.67.1"
```

Se o comando `python` não estiver disponível, verifique se o ambiente virtual foi ativado corretamente.

---

## 8. Configurar o dbt e o DuckDB

O DuckDB será utilizado para executar as consultas analíticas. O dbt organizará as transformações em modelos SQL e executará os testes de qualidade dos dados.

Caso queira executar o dbt diretamente no ambiente local, instale:

```bash
python -m pip install \
  "dbt-core==1.12.5" \
  "dbt-duckdb==1.11.0"
```

Verifique a instalação:

```bash
dbt --version
```

### Configurar o projeto dbt

Crie o arquivo:

`dbt/nyc_taxi/dbt_project.yml`

Configure o projeto com o nome `nyc_taxi`, a versão desejada e o perfil `nyc_taxi`.

O perfil de conexão deve ser definido em:

`config/dbt/profiles.yml`

Para a execução final dentro do Docker, utilize a configuração:

```yaml
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

O caminho acima corresponde ao sistema de arquivos **dentro do contêiner do Airflow**, e não diretamente ao macOS.

Essa distinção é importante porque os caminhos do computador local nem sempre existem dentro de um contêiner.

### Criar o modelo de staging

Crie o arquivo:

`dbt/nyc_taxi/models/staging/stg_yellow_taxi.sql`

O modelo deve ler o conjunto de dados Parquet disponibilizado no diretório do projeto montado dentro do Docker:

```sql
select *
from read_parquet(
    '/opt/airflow/project/data/raw/yellow_tripdata_2025-01.parquet'
)
```

Esse modelo constitui a primeira camada de transformação dos dados brutos.

O caminho precisa corresponder ao local em que o projeto está montado no contêiner.

### Criar o modelo analítico

Crie o arquivo:

`dbt/nyc_taxi/models/marts/taxi_summary.sql`

Esse modelo deve utilizar `stg_yellow_taxi` como origem e produzir um resumo analítico das corridas.

A agregação pode utilizar informações como quantidade de corridas, distância percorrida, valores das tarifas ou dados de pagamento, conforme a análise pretendida.

Os nomes das colunas devem corresponder aos campos realmente disponíveis no conjunto de dados e ao modelo de staging.

### Adicionar testes de qualidade

Configure testes de esquema para verificar, entre outros aspectos, se os seguintes campos não contêm valores nulos:

* `VendorID`;
* `fare_amount`;
* `trip_distance`.

Esses testes ajudam a verificar pressupostos sobre os dados antes de considerar as transformações bem-sucedidas.

### Validar o dbt

Com o conjunto de dados disponível e o perfil configurado, execute:

```bash
dbt debug \
  --project-dir dbt/nyc_taxi \
  --profiles-dir config/dbt
```

Em seguida, execute os modelos e os testes:

```bash
dbt build \
  --project-dir dbt/nyc_taxi \
  --profiles-dir config/dbt
```

Se o dbt não encontrar o arquivo Parquet, verifique os caminhos, o diretório de trabalho e a localização real dos dados antes de alterar os modelos SQL.

---

## 9. Criar a imagem Docker do Airflow

O projeto final utiliza o Airflow em contêineres Docker para tornar o ambiente de orquestração mais reproduzível.

Crie o arquivo `Dockerfile.airflow` na raiz do projeto:

```dockerfile
FROM apache/airflow:3.3.2-python3.12

USER airflow

RUN python -m venv /home/airflow/pipeline-venv && \
    /home/airflow/pipeline-venv/bin/pip install --no-cache-dir \
    'dvc[s3]==3.67.1' \
    'dbt-core==1.12.5' \
    'dbt-duckdb==1.11.0'

ENV PATH="/home/airflow/pipeline-venv/bin:${PATH}"
```

Essa imagem disponibiliza as ferramentas necessárias para executar as tarefas da pipeline.

A criação de um ambiente virtual próprio dentro da imagem evita modificar diretamente o ambiente Python-base do Airflow.

### Por que criar uma imagem personalizada?

A imagem padrão do Airflow não necessariamente contém o DVC, suas dependências S3 ou o dbt com o adaptador do DuckDB.

Sem essas dependências, as tarefas que executam comandos `dvc` ou `dbt` podem falhar, mesmo que os comandos funcionem no computador local.

---

## 10. Configurar o Airflow com Docker Compose

Crie o arquivo `docker-compose.airflow.yml` para definir os serviços do Airflow e suas dependências.

A arquitetura final utiliza:

* PostgreSQL 16 para os metadados do Airflow;
* servidor de API do Airflow;
* processador de DAGs;
* scheduler;
* triggerer;
* serviço de inicialização.

Os serviços devem utilizar imagens compatíveis e compartilhar a montagem do diretório do projeto quando necessário.

O projeto é montado dentro dos contêineres em:

```text
/opt/airflow/project
```

Por isso, o perfil do dbt e o modelo SQL utilizam caminhos internos do Docker.

### Configurar o remote do DVC para o Docker

O remote usado dentro dos contêineres precisa utilizar o nome do serviço MinIO na rede do Docker.

Configure-o:

```bash
dvc remote add minio_docker s3://dvc-storage
dvc remote modify minio_docker endpointurl http://minio:9000
```

Existem dois endereços diferentes:

* No computador local: `http://localhost:9000`
* Na rede Docker: `http://minio:9000`

O nome `minio` permite que os contêineres se comuniquem pela rede definida no Docker Compose.

As credenciais precisam estar disponíveis para o processo DVC executado dentro do Airflow, por meio da configuração local apropriada ou de variáveis de ambiente.

Não inclua credenciais no Git.

### Configurar a execução de tarefas do Airflow

Verifique se os serviços do Airflow recebem as configurações necessárias à execução das tarefas, incluindo a URL do servidor de execução da API:

```text
AIRFLOW__CORE__EXECUTION_API_SERVER_URL=http://airflow-api-server:8080/execution/
```

Utilize os arquivos finais do Docker Compose como referência para os nomes dos serviços, as variáveis de ambiente, os volumes, as portas e as dependências.

Valide a configuração:

```bash
docker compose \
  -f docker-compose.yml \
  -f docker-compose.airflow.yml \
  config
```

Construa a imagem personalizada:

```bash
docker compose \
  -f docker-compose.yml \
  -f docker-compose.airflow.yml \
  build airflow-api-server
```

Se os demais serviços do Airflow utilizarem a mesma imagem personalizada, confirme que estão configurados para utilizá-la.

Inicialize o Airflow:

```bash
docker compose \
  -f docker-compose.yml \
  -f docker-compose.airflow.yml \
  up airflow-init
```

Depois da inicialização, inicie os serviços em segundo plano:

```bash
docker compose \
  -f docker-compose.yml \
  -f docker-compose.airflow.yml \
  up -d
```

Confira o estado dos serviços:

```bash
docker compose \
  -f docker-compose.yml \
  -f docker-compose.airflow.yml \
  ps
```

Acesse a interface do Airflow:

http://localhost:8080

Utilize a conta configurada durante a inicialização.

---

## 11. Criar o DAG do Airflow

Crie o arquivo:

`dags/nyc_taxi_pipeline.py`

O DAG organiza o fluxo em três tarefas sequenciais:

1. `dvc_pull`;
2. `validate_parquet`;
3. `dbt_build`.

Utilize a implementação final:

```python
from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator


with DAG(
    dag_id="nyc_taxi_pipeline",
    description="DVC + MinIO + dbt + DuckDB",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["dvc", "dbt", "duckdb"],
) as dag:

    pull_data = BashOperator(
        task_id="dvc_pull",
        bash_command=(
            "cd /opt/airflow/project && "
            "dvc pull -r minio_docker "
            "data/raw/yellow_tripdata_2025-01.parquet.dvc"
        ),
    )

    validate_data = BashOperator(
        task_id="validate_parquet",
        bash_command=(
            "test -s "
            "/opt/airflow/project/data/raw/yellow_tripdata_2025-01.parquet"
        ),
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            "cd /opt/airflow/project && "
            "dbt build --project-dir dbt/nyc_taxi "
            "--profiles-dir config/dbt"
        ),
    )

    pull_data >> validate_data >> dbt_build
```

### Tarefa 1: `dvc_pull`

Recupera do MinIO a versão do conjunto de dados registrada pelo DVC.

### Tarefa 2: `validate_parquet`

Verifica se o arquivo esperado existe e não está vazio.

### Tarefa 3: `dbt_build`

Executa os modelos dbt e os testes configurados.

A expressão:

```python
pull_data >> validate_data >> dbt_build
```

define a dependência entre as tarefas e garante a ordem de execução.

O DAG utiliza `schedule=None`, portanto é executado manualmente, sem agendamento periódico.

---

## 12. Configurar o Git e ignorar arquivos locais

Crie ou atualize o arquivo `.gitignore` para excluir dados gerados, credenciais e artefatos locais.

Exemplo:

```gitignore
.venv/
__pycache__/
*.py[cod]

.env
.DS_Store

data/warehouse/
*.duckdb
*.duckdb.wal

logs/

dbt/nyc_taxi/target/
dbt/nyc_taxi/logs/
dbt/nyc_taxi/dbt_packages/
dbt/nyc_taxi/.user.yml

config/dbt/.user.yml

.dvc/config.local
```

Os seguintes arquivos devem ser versionados:

* código-fonte;
* definições dos DAGs;
* modelos SQL e configurações do dbt;
* arquivos Docker;
* metadados do DVC, incluindo os arquivos `.dvc`;
* documentação.

Não versione:

* credenciais;
* arquivos `.env` com segredos;
* credenciais locais do DVC;
* bancos DuckDB gerados;
* arquivos de dados grandes já gerenciados pelo DVC.

Verifique se os arquivos sensíveis estão sendo ignorados:

```bash
git check-ignore .dvc/config.local .env
```

Se um segredo já tiver sido enviado ao Git, adicioná-lo ao `.gitignore` não o remove do histórico. Nesse caso, revogue ou troque a credencial exposta e trate o histórico do repositório adequadamente.

---

## 13. Executar a pipeline completa

Antes de executar o DAG, confirme que:

* o MinIO está funcionando;
* o bucket existe;
* o remote DVC contém o conjunto de dados;
* o Airflow está em execução;
* a imagem personalizada contém DVC e dbt;
* o caminho do Parquet corresponde à montagem do contêiner;
* o perfil do dbt aponta para o caminho esperado do DuckDB.

Na interface do Airflow:

1. Localize `nyc_taxi_pipeline`.
2. Ative o DAG, se necessário.
3. Execute-o manualmente.
4. Abra a execução e inspecione o estado de cada tarefa.
5. Confirme que as três tarefas foram concluídas com sucesso.

A sequência esperada é:

```text
dvc_pull
    |
    v
validate_parquet
    |
    v
dbt_build
```

Uma execução bem-sucedida confirma a integração entre recuperação dos dados, validação, transformação e testes.

---

## 14. Validar os resultados

O banco DuckDB é criado dentro do contêiner no caminho:

```text
/opt/airflow/project/data/warehouse/nyc_taxi.duckdb
```

Como `data/warehouse/` está excluído do Git, o banco é um artefato local gerado durante a execução.

Verifique os resultados do dbt nos logs das tarefas do Airflow. No projeto concluído, os três testes `not_null` configurados foram aprovados.

Para inspecionar o banco, utilize um cliente DuckDB ou um pequeno script Python com o pacote `duckdb` instalado.

Por exemplo:

```python
import duckdb

connection = duckdb.connect(
    "data/warehouse/nyc_taxi.duckdb",
    read_only=True,
)

print(
    connection.execute(
        "SHOW TABLES"
    ).fetchall()
)

connection.close()
```

Execute esse script no computador local somente se o banco estiver disponível nesse caminho. Caso contrário, inspecione-o dentro de um contêiner que tenha acesso ao volume do projeto.

A validação final deve confirmar que as relações esperadas pelo dbt existem e que o modelo analítico pode ser consultado.

---

## 15. Problemas encontrados e soluções

### O DVC não consegue acessar o S3

Instale o suporte S3 no ambiente Python utilizado pelo DVC:

```bash
python -m pip install "dvc[s3]==3.67.1"
```

### A imagem do MinIO não pode ser baixada

A disponibilidade de imagens e o comportamento dos registros podem mudar. Verifique a imagem configurada, o acesso ao registro e a documentação correspondente.

Não presuma que trocar de imagem MinIO seja equivalente sem conferir os requisitos de inicialização e as variáveis de ambiente.

### O DVC funciona localmente, mas falha no Airflow

O computador local e os contêineres utilizam endereços de rede diferentes.

* Computador local: `http://localhost:9000`
* Rede Docker: `http://minio:9000`

O remote correto e as credenciais precisam estar disponíveis no ambiente que executa o comando.

### O dbt não encontra o arquivo Parquet

Confira a montagem do diretório do projeto e o caminho referenciado pelo modelo SQL.

Um caminho do macOS, como `/Users/...`, não passa a existir automaticamente dentro do contêiner.

### As tarefas do Airflow falham mesmo após a construção da imagem

Inspecione os logs das tarefas e verifique a configuração da API de execução, as dependências da imagem e a comunicação entre os serviços.

### O banco DuckDB não aparece no computador local

Confira se o contêiner está escrevendo no diretório montado e se o caminho utilizado corresponde ao volume esperado.

---

## 16. Publicar o projeto no GitHub

Confira as alterações antes de criar o commit:

```bash
git status
git diff
```

Adicione e versione os arquivos:

```bash
git add .
git commit -m "Build end-to-end NYC taxi data pipeline"
```

Crie um repositório no GitHub e configure o remote, caso ainda não exista:

```bash
git remote add origin \
  https://github.com/SEU_USUARIO/nyc-taxi-data-engineering.git
```

Se o remote já estiver configurado, confira-o antes de adicionar outro:

```bash
git remote -v
```

Envie a branch principal:

```bash
git branch -M main
git push -u origin main
```

O repositório publicado deve conter o código, os arquivos de configuração compartilháveis, os metadados do DVC e a documentação, mas não as credenciais nem os arquivos gerados localmente.

---

## 17. Reproduzir o projeto em outra máquina

Um novo clone do Git não recupera automaticamente todo o estado local do projeto.

Quem for reproduzi-lo precisará de:

* acesso ao remote DVC que contém os dados, ou de um remote próprio;
* credenciais próprias do MinIO;
* suas próprias variáveis de ambiente e configuração inicial do Airflow;
* Docker e recursos suficientes para construir e executar os serviços.

Clone o repositório e siga este tutorial desde o início, criando as configurações locais necessárias.

Se o bucket MinIO original não estiver acessível, crie um novo bucket, baixe o Parquet de origem, adicione-o ao DVC e envie-o para o novo remote antes de executar o DAG.

Nunca publique chaves de acesso ou senhas nas instruções de reprodução.

---

## 18. O que este projeto demonstra

Este projeto demonstra a integração de ferramentas de engenharia de dados em uma pipeline local reproduzível:

* Versionamento de dados com DVC.
* Armazenamento de objetos com MinIO.
* Transformações SQL e testes com dbt.
* Armazenamento e consultas analíticas com DuckDB.
* Orquestração com Apache Airflow.
* Infraestrutura em contêineres com Docker Compose.
* Versionamento e documentação com Git.

O principal objetivo de aprendizagem não é apenas executar comandos isolados, mas compreender como configurações, caminhos, dependências, armazenamento e orquestração precisam funcionar em conjunto para que uma pipeline completa seja executada corretamente.
