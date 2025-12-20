
# Extract People Data

Este projeto realiza a extração periódica de dados de pessoas a partir de uma API pública e armazena os resultados em um bucket MinIO no formato JSON, organizando os arquivos por data e hora.

## Funcionalidades Principais

- **Extração de Dados:**
	- Consome dados da API [randomuser.me](https://randomuser.me/api/) para obter informações fictícias de pessoas.
- **Armazenamento em MinIO:**
	- Salva os dados extraídos em um bucket MinIO, criando o bucket automaticamente caso não exista.
	- Os arquivos são salvos em formato JSON, organizados em pastas por ano, mês, dia e hora.
- **Execução Periódica:**
	- O processo de extração e upload é executado continuamente a cada 30 segundos.
- **Tratamento de Erros:**
	- Lida com erros de conexão HTTP e erros do MinIO, exibindo mensagens informativas no console.

## Como Executar

1. **Pré-requisitos:**
	 - Python 3.x
	 - MinIO em execução (padrão: `localhost:9000`)
	 - Instale as dependências:
		 ```bash
		 pip install -r requirements.txt
		 ```

2. **Configuração:**
	 - O script utiliza as seguintes credenciais padrão para o MinIO:
		 - Usuário: `datalake`
		 - Senha: `datalake`
	 - O bucket padrão é `raw`.

3. **Execução:**
	 - Execute o script principal:
		 ```bash
		 python main.py
		 ```

## Estrutura dos Arquivos Salvos

Os arquivos JSON são salvos no bucket MinIO seguindo a estrutura:

```
year=YYYY/month=MM/day=DD/hour=HH/person_MMSS.json
```

Exemplo:
```
year=2025/month=12/day=20/hour=15/person_3045.json
```

## Exemplo de Saída

Um exemplo de arquivo gerado pode ser encontrado em [sample.json](sample.json), que demonstra o formato dos dados extraídos da API e salvos no MinIO.

## Personalização

- Para alterar o intervalo de extração, modifique a variável `interval` em `main.py`.
- Para mudar o endpoint ou credenciais do MinIO, edite as variáveis correspondentes em `main.py`.

## Licença

Este projeto é de uso livre para fins educacionais e demonstração.
