# Trabalho de Conclusão de Curso

Esse projeto contem a implementão do trabalho de conclusão de curso de Ciência da Computação na Universiade Federal do Espírito Santo campus Alegre, cujo o tema: 

### IMPLEMENTAÇÃO DE UM DATA MART PARA ESTUDO DE EVASÃO ESTUDANTIL NA UFES CAMPUS ALEGRE-ES

Disponível em: link do trabalho

Autor: Eloy de Freitas Almeida 

- https://github.com/eloy-freitas
- https://www.linkedin.com/in/eloy-freitas-0356801bb/

Esse projeto foi desenvolvido utilizando linguagem de programação python para criação de scripts ETL para alimentar o Datamart criado.

O fluxo desenvolvido é configurado e orquestrado em uma infraestrutura de microserviços do Apache Airflow (https://airflow.apache.org/) 


### Requisitos de sistemas

- Docker versão 24.0.7, build afdd53b
- Sistema Operacional Linux x86_64 derivados do Debian

# Tutorial

## Configuração de ambiente
Para execução bem sucedida é preciso satisfazer os requisitos de sistemas. Utilizando
um sistema operacional Linux baseado em Debian é preciso realizar a instalação do
serviço Docker para execução dos serviços do Apache Airflow

1. Instalação do docker: https://docs.docker.com/engine/install/debian/


## Instalação do Sistema
Após a satisfação dos requisitos de sistemas é preciso fazer o download do código
fonte do projeto.
Em um terminal execute o comando para fazer o download:
```
git clone git@github.com:eloy-freitas/TCC2.git && cd TCC2;
```

Existem três scripts de configuração do sistema que estão na raiz do diretório do
projeto:
- start.sh - Compila o código fonte e executa os serviços;
- stop.sh - Para parar os serviços;
- nuke.sh - Para parar e remover os serviços;

Todos eles utilizam o arquivo ”config_envs.json” que possui as seguintes variáveis para
configuração do sistema:

- AIRFLOW_WORKSPACE = Pasta de trabalho do airflow;
- SUBDIRS = Diretórios do airflow;
- DOCKER_IMAGE_OWNER = Nome da proprietário da imagem do docker;
- DOCKER_IMAGE_NAME = Nome da imagem;
- DOCKER_IMAGE_VERSION = Versão da imagem;
- DOCKER_FILE = caminho do arquivo Dockerfile;
- DOCKER_COMPOSE_FILE = caminho do arquivo Docker compose;

## Execução

O script start.sh é responsável por preparar iniciar os serviços do sistema. Ele executa
as seguintes tarefas:
- Instalação de dependências do projeto;
- Criação dos diretórios utilizados pelo ambiente do Airflow;
- Compilar a imagem do Airflow com o código fonte do projeto;
- Se existir uma execução em andamento, ela é finalizada;
- Execução do sistema;
- Configuração das conexões com o banco de dados do Data Mart;

Para executar o script basta executar:
```
./start.sh
```
Vale ressaltar que os serviços do Airflow utilizam as seguintes portas do host:
57
- 5432 - Banco de dados de metadados do Airflow
- 10001 - Banco de dados do Data Warehouse
- 8080 - Interface de usuário do Airflow
- 5555 - Orquestrador dos serviços Airflow


## Fornecendo dados

Para fornecer arquivos para extração de dados do ETL, basta disponibilizá-los no diretório airflow/data.

## Parar

O script `stop.sh` é responsável por parar todos os containers do ambiente.

Para executar o ambiente basta executar o script:
```sh
./stop.sh
```

## Excluindo
### Antes de fazer essa operação faça backup do Data Wharehouse e também dos arquivos de dados no diretórios airflow/data, pois tudo será perdido.

O script `nuke.sh` é responsável por:
- Parar e excluir todos os containers
- Apagar o ambiente de trabalho do airflow

Para executar o ambiente basta executar o script:
```sh
sudo ./nuke.sh
```