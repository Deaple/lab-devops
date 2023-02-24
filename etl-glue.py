---
categories: ["Examples", "Placeholders"]
tags: ["test","docs"] 
title: "Provisionamento de Gateway"
linkTitle: "Provisionamento de Gateway"
weight: 2
description: >
  Como configurar o API Gateway AWS
---
## OpenAPI

O arquivo OpenAPI se encontra na raiz do projeto, como **nome-do-repositorio.yaml**

### 1. Propriedades da conta

Na pasta /AWS/ altere o arquivo **"propriedades-aws.json"** contendo os ids das contas da AWS dos 3 ambientes junto com a região em que estão cadastrados.

No campo **"NomeRepositorio"** deve ser preenchido com o mesmo nome que o repositório git.


```json
{
  "idContaAWS_DES": "782583110898",
  "idContaAWS_HOM": "138374051704",
  "idContaAWS_PROD": "738764479326",
  "AWSAccount_Region_DES": "sa-east-1",
  "AWSAccount_Region_HOM": "sa-east-1",
  "AWSAccount_Region_PROD": "sa-east-1",
  "Repositorio": "local",
  "NomeRepositorio": "{nome do repositório no GitHub}",
  "GatewayRoleName": "iamsr/role-gateway"
  //...
}
```

## 2. Arquivos de parâmetros

Para o provisionamento do API Gateway na AWS deve ser preenchido o arquivo de parâmetros no formato JSON, que será utilizado pelo _template cloudformation_.

Há 3 arquivos de parâmetros, um para cada ambiente, localizado no repositório de contrato:
- Desenvolvimento: AWS/parameters-**dev**.json
- Homologação: AWS/parameters-**hom**.json
- Produção: AWS/parameters-**prod**.json

Após o preenchimento, o arquivo será validado pela automação da governança copiado para a pasta "AWS/" de forma automática para que a esteira de virtualização da AWS possa criar o Gateway.

Abaixo os campos que podem ser preenchidos de acordo com o ambiente:

<h4><span style="color:red"><b>obs.:campos com * são obrigatórios e caso não sejam preenchidos o pipeline de provisionamento não realiza o deploy do gateway.</b></span></h4>


|Identificador|Tipo|Descrição|Pré Requisito |Default|Exemplo|
|:----|:----|:----|:----|:----|:----|
|swaggerName*|String|Nome do Swagger que será gerado|N/A|Sim, não alterar|Develop/receb-cash-aws-1.5.0.json|
|apiGatewayStageName*|String|Nome do ambiente em que será gerado o gateway.|N/A|Sim, não alterar|dev, hom, prod|
|BranchName*|String|Nome da branch em que será disponibilizado os artefatos|N/A|Sim, não alterar|development, master|
|DevToolsAccountParameter*|String|Número da conta de tools do cliente|Conta deve existir|N/A|DevToolsAccountParameter": "417255830331|
|EndpointConfigurationType|String|Tipo de endpoint do gateway: PRIVATE para gateways internos (visíveis somente para a rede interna/vpc preenchida anteriormente) ou REGIONAL para gateways externos (visíveis via internet)|N/A|PRIVATE|EndpointConfigurationType":"PRIVATE" ou "EndpointConfigurationType": "REGIONAL|
|GatewayType*|String|Nome do tipo de gateway|Nome do tipo de gateway|AWS|- aws (interno)<br />- aws (externo)<br />- binint-aws (binário interno)<br />- binext-aws (binário externo)<br />- tecint-aws (técnico interno)<br />- tecext-aws (técnico externo)<br />- bff-aws (backend for frontend)|
|NetworkLoadBalancers|String|Caso haja mais de um NetworkLoadBalancer atrás do gateway, replicar esse campo (uma linha para cada NLB). Há um template disponível com os arquivos que precisam ser configurados, contendo 3 load balancers configurados: CloudFormation: <url nova>. parameters-dev (a mesma configuração deve ser feita nos arquivos parameters-hom e parameters-prod): <url-nova>. Backends que farão integração com o NLB: <url nova>|NetworkLoadBalancers devem existir nas contas| |NetworkLoadBalancers devem existir nas contas|
|PrivateSubnetOne*|String|Subnet da AZ1 relacionada a VPC já preenchida|Subnet deve existir na conta do respectivo ambiente|N/A|PrivateSubnetOne": "subnet-0102ef4d044cbac14|
|PrivateSubnetTwo*|String|Subnet da AZ2 relacionada a VPC já preenchida|Subnet deve existir na conta do respectivo ambiente|N/A|PrivateSubnetTwo": "subnet-0102ef1d033cbac49|
|VPCCIDR*|String|CIDR relacionado ao VPCID já preenchido|VPC deve existir na conta do respectivo ambiente|N/A|VPCIDR": "172.23.40.0/23|
|VPCID*|String|Id da VPC em que será provisionado o gateway e o lambda autorizador| |N/A|VPC deve existir na conta do respectivo ambiente|
|GatewayRoleName*|String|Role do API Gateway usada para log|Role de log deve exsitir na conta do respectivo ambiente|N/A|GatewayRoleName": "iamsr/role-gateway|


## 3. Configurando backends (Integrations)

A seguir, escolha um tipo de integração conforme o seu tipo de backend. Leia a breve descrição de cada um tipos para entender se ele atende à sua especificação de arquitetura.

- [AWS Services](http://localhost:1313/pt-br/docs/exposi%C3%A7%C3%A3o-de-apis/getting-started/cria%C3%A7%C3%A3o-de-reposit%C3%B3rio-e-pipeline-aws/provisionamento-de-gateway/#31-aws-services): integração direta do API Gateway com serviços da AWS através de suas APIs, sem backened intermediário. [Saiba mais.](link)
- [HTTP](http://localhost:1313/pt-br/docs/exposi%C3%A7%C3%A3o-de-apis/getting-started/cria%C3%A7%C3%A3o-de-reposit%C3%B3rio-e-pipeline-aws/provisionamento-de-gateway/#32-http): integração entre o API Gateway e uma URI de backend, que responda requisições HTTP.
- [HTTP_PROXY](http://localhost:1313/pt-br/docs/exposi%C3%A7%C3%A3o-de-apis/getting-started/cria%C3%A7%C3%A3o-de-reposit%C3%B3rio-e-pipeline-aws/provisionamento-de-gateway/#33-http_proxy): integração entre o API Gateway e uma URI de backend, que responda requisições HTTP.
- [Lambda Custom](http://localhost:1313/pt-br/docs/exposi%C3%A7%C3%A3o-de-apis/getting-started/cria%C3%A7%C3%A3o-de-reposit%C3%B3rio-e-pipeline-aws/provisionamento-de-gateway/#34-lambda-custom)
- [Lambda Proxy](http://localhost:1313/pt-br/docs/exposi%C3%A7%C3%A3o-de-apis/getting-started/cria%C3%A7%C3%A3o-de-reposit%C3%B3rio-e-pipeline-aws/provisionamento-de-gateway/#36-mock)
- [MOCK](http://localhost:1313/pt-br/docs/exposi%C3%A7%C3%A3o-de-apis/getting-started/cria%C3%A7%C3%A3o-de-reposit%C3%B3rio-e-pipeline-aws/provisionamento-de-gateway/#36-mock)
- [Private Integration](http://localhost:1313/pt-br/docs/exposi%C3%A7%C3%A3o-de-apis/getting-started/cria%C3%A7%C3%A3o-de-reposit%C3%B3rio-e-pipeline-aws/provisionamento-de-gateway/#37-private-integration)

Para saber mais sobre os tipos de integração suportados pelo API Gateway consulte [este link](link).

#### Pré-requisitos
A fim de simplificar a leitura, é importante definir os pré-requisitos que são comuns à todos os tipos de integração do API Gateway AWS.

Para configurar um API Gateway através da integração, é preciso primeiramente, referenciar os arquivos de ingração do contrato. O arquivo do contrato encontra-se na raiz do repositório e leva o mesmo nome, como "**nome-repositório-v1-aws.yaml"**.

No diretório AWS/AWS-backends/backend-{DEV, HOM, PRD}/ do repositório, estão localizados os arquivos de integração para os ambientes de desenvolvimento, homologação e produção, respectivamente.

Os arquivos de integração dos métodos são armazenados no formato JSON e são referenciados pela tag "**x-amazon-apigateway-integration-config**" no arquivo de contrato. Recomenda-se utilizar o nome do método e da rota para nomear os arquivos, por exemplo "get-consulta-saldo.json" que descreve o método GET da rota /consulta-saldo. A seguir, um exemplo da rota referenciada no arquivo de contrato.

```yaml
  /rota:
    post:
      tags:
        - rota
      operationId: post/rota
      description: rota de exemplo
      responses:
        '200':
          content:
            application/json:
              schema:
                $ref: '#/components/schema/rota'
              examples:
                200-rota:
                  $ref: '#/components/examples/200-rota'
          description: descrição da response 200
          headers:
            Content-Location:
              description: descrição do header
              schema:
                type: string
      parameters:
        - in: header
          name: x-apiKey
          description: Chave da API utilizada para autorizar o consumo de uma aplicação
          schema:
            type: string
      requestBody:
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/rotareq'
      security:
        - none: []
      x-amazon-apigateway-integration-config: post-rota.json
```

Neste exemplo a rota "/rota" do arquivo de contrato tem um método POST com toda a estrutura de parâmetros, body, headers e resposta mapeados e a integração com o backend é referenciada no arquivo **post-rota.json**, que se encontra no diretório AWS/AWS-backends/backend-{DEV, HOM, PRD}/ (obs: não é preciso adicionar o caminho, a pipeline se encarrega desta tarefa). A partir daqui, iremos referenciar os arquivos de configuração de integração dos métodos, diferenciando as configuração específicas para cada tipo de arquitetura.

### 3.1 AWS Services

<h5><span style="color:red"><b>obs.: Não recomendado pela Integração Digital.</b></span></h5>

Para este tipo de integração, é preciso configurar tanto a requisição quanto a resposta de Integração.

Pontos importantes para esta integração:
- O parâmetro **type** deve ter o valor "**aws_proxy**", o valor "aws" deve ser utilizado apenas para integrações Lambda Custom ou AWS Services.

Neste exemplo, iremos configurar a integração do API Gateway com a comunicação direta na API do serviço S3, através da action **PutObject** da API. Como referência, foi utilizada a **[API do S3](https://docs.aws.amazon.com/AmazonS3/latest/API/API_Operations_Amazon_Simple_Storage_Service.html)**, para entender quais parâmetros, tipos e resposta esperada. Para cada serviço que se deseja integrar, consulte a documentação da API.

A integração irá receber a requisição POST para inserção de arquivo e os parâmetros bucket com o nome da **bucket** e **key** com o caminho onde o arquivo será armazenado. 

```json
{
    "type": "aws",
    "httpMethod": "POST",
    "uri": "arn:aws:apigateway:sa-east-1:0123456789012:s3:path/{bucket}/{key}",
    "passthroughBehavior": "when_no_match",
    "contentHandling": "CONVERT_TO_BINARY",
    "requestParameters": {
      "integration.request.path.bucket": "method.request.path.bucket",
      "integration.request.path.key": "method.request.path.key"
    },
    "integrationResponses": {
      "200": {
        "statusCode": "200",
        "responseParameters": {
          "method.response.header.Content-Type": "'application/octet-stream'",
          "method.response.header.Content-Disposition": "'attachment;filename=\"\"'"
        }
      }
    }
  },
  "parameters": {
    "bucket": {
      "type": "string",
      "required": true
    },
    "key": {
      "type": "string",
      "required": true
    }
  },
  "responses": {
    "200": {
      "description": "Successful operation",
      "headers": {
        "Content-Type": {
          "type": "application/json"
        },
        "Content-Disposition": {
          "type": "string"
        }
      }
    }
}
```
Neste objeto é definido o **integration request**, com os parâmetros aceitos pela API do S3. Os parâmetros devem ser tratados no Mapping da integração:

```json
"requestParameters": {
      "integration.request.path.bucket": "method.request.path.bucket",
      "integration.request.path.key": "method.request.path.key"
    }
```


Aqui é definido uma **integration response**, com o tipo de resposta esperado pelo nosso "backend", que é a API do S3:

```json
"integrationResponses": {
      "200": {
        "statusCode": "200",
        "responseParameters": {
          "method.response.header.Content-Type": "'application/json'",
        }
      }
    }
```

Ao finalizr, o arquivo deve ser armazenado como, ex :**post-salvar-documento.json** no diretório de backend(AWS/Backends/backend-{DEV,HOM,PROD})

### 3.2 HTTP

<h5><span style="color:red"><b>obs.: Considerada um Anti-pattern para gateways externos por permitir conexões com URLs externas. O recomendado é que workloads externos saiam para a internet via SecOutbound. Logo, para este modelo não utilize o connectionType: "INTERNET", por permitir consumo de URL externa via API Gateway.</b></span></h5>


No caso de um gateway interno, utilizaremos a configuração a seguir para configurar a integração http. O **type** deve ser configurado como "http" e o parâmetro **uri** deve apontar para o endereço HTTP do backend (ou NLB). Neste caso, a integração é customizada e deve-se informar tanto a integration request e response.

```json
{
  "passthroughBehavior": "when_no_match",
  "connectionType": "VPC_LINK",
  //pode-se passar a string ou utilizar a variável de stage com o valor da VPC link preenchido.
  "connectionId": "${stageVariable.vpcLink}",
  "timeoutMillis": 14000,
  "uri": "https://des.get-id-teste.com",
  "type": "http",
  "httpMethod": "POST",
  "integrationResponses": {
    "default":{
      "statusCode": "200"
    },
    "201": {
      "statusCode": "201"
    }
  },
  "requestParameters": {
    "integration.request.queryString.stage": "method.request.queryString.version",
    "integration.request.header.x-userid": "method.request.header.x-user-id",
    "integration.request.path.op": "method.request.path.service",
    "integration.request.header.Content-Type": "'application/json'"
  }
}
```
### 3.3 HTTP_PROXY

Para uma integração com o backend através via http proxy, é preciso configurar o **connectionType** como "PutObject", o **connectionId** com o **ID da VPC Link** da conta do respectivo ambiente, **uri** com o endereço do backend ou de seu NLB/ALB e por fim, o **type** como "**http_proxy**". Para este tipo de configuração, não é necessário configurar o integration response, pois o API Gateway repassa a requisição do cliente para o endpoint HTTP e repassa a resposta do endpoint ao cliente. 


```json
{
  "passthroughBehavior": "when_no_match",
  "connectionType": "VPC_LINK",
  "connectionId": "9h5iih",
  "timeoutMillis": 14000,
  "uri": "https://des.get-id-teste.com",
  "type": "http_proxy",
  "httpMethod": "POST",
  "responses": {
    "default":{
      "statusCode": "200"
    },
    "201": {
      "statusCode": "201"
    }
  },
  "requestParameters": {
    "integration.request.queryString.stage": "method.request.queryString.version",
    "integration.request.header.x-userid": "method.request.header.x-user-id",
    "integration.request.path.op": "method.request.path.service"
  }
}
```

Ao finalizr, o arquivo deve ser armazenado como, ex :**post-salvar-documento.json** no diretório de backend(AWS/Backends/backend-{DEV,HOM,PROD})

### 3.4 Lambda Custom

Para a configuração da integração lambda custom, o parâmetro **uri** deve apontar para o caminho da lambda na conta que deseja tratar as requisições, o valor do **type** deve ser **aws** e o parâmetro **connectionType** deve ser "INTERNET". A lambda custom deve conter o integration response e request, pois o API Gateway não faz o repasse para a mesma.

```json
{
  "passthroughBehavior": "when_no_match",
  "connectionType": "INTERNET",
  "timeoutMillis": 14000,
  "uri": "arn:aws:apigateway:sa-east-1:lambda:path/2015-03-31/functions/arn:aws:lambda:sa-east-1:0123456789012:functions:mylambda/invoctations",
  "type": "aws",
  "httpMethod": "POST",
  "responses": {
    "default":{
      "statusCode": "200"
    },
    "201": {
      "statusCode": "201"
    }
  },
  "requestParameters": {
    "integration.request.queryString.stage": "method.request.queryString.version",
    "integration.request.header.x-userid": "method.request.header.x-user-id",
    "integration.request.path.op": "method.request.path.service"
  }
}
```

Ao finalizr, o arquivo deve ser armazenado como, ex :**post-salvar-documento.json** no diretório de backend(AWS/Backends/backend-{DEV,HOM,PROD})

### 3.5 Lambda Proxy

Semelhante à lambda custom, a configuração a **uri** para apontar o caminho da lambda na conta que deseja tratar as requisições, porém o valor do **type** deve ser **aws_proxy** e o parâmetro **connectionType** deve ser "INTERNET". Para integração lambda proxy, não é necessário configurar o integration request e response, pois o API Gateway faz o repasse da requisição para o endpoint lambda e resposta da mesma ao cliente.


```json
{
  "passthroughBehavior": "when_no_match",
  "connectionType": "INTERNET",
  "timeoutMillis": 14000,
  "uri": "arn:aws:apigateway:sa-east-1:lambda:path/2015-03-31/functions/arn:aws:lambda:sa-east-1:0123456789012:functions:mylambda/invoctations",
  "type": "aws_proxy",
  "httpMethod": "POST",
  "responses": {
    "default":{
      "statusCode": "200"
    },
    "201": {
      "statusCode": "201"
    }
  },
  "requestParameters": {
    "integration.request.queryString.stage": "method.request.queryString.version",
    "integration.request.header.x-userid": "method.request.header.x-user-id",
    "integration.request.path.op": "method.request.path.service"
  }
}
```

Ao finalizr, o arquivo deve ser armazenado como, ex :**post-salvar-documento.json** no diretório de backend(AWS/Backends/backend-{DEV,HOM,PROD})

### 3.6 MOCK

O tipo de integração MOCK é utilizado quando deseja-se simular um backend para fins de testes e agilizar o desenvolvimento enquanto o backend ainda não está totalmente disponível. Também é utilizado para respostas padrão, como no caso de CORS, onde é necessário um método OPTIONS (ou pre-flight) e que deve retornar métodos e headers específicos. A configuração deste tipo de integração é a seguinte:

```json
{
 "passthroughBehavior": "when_no_match",
  "connectionType": "MOCK",
  "timeoutMillis": 14000,
  "type": "http_proxy",
  "httpMethod": "POST",
  "requestTemplates": {
    "application/json": "{\"statusCode\": 200}"
  },
  "responses": {
    "default":{
      "statusCode": "200",
      "responseTemplates": {
        "application/json": "{\"message\":\"Usuário criado com sucesso.\"}"
      }
    }
  }
}
```

No exemplo acima foi utilizado uma resposta personalizada com status 200 ao método POST, mas é possível configurar outros tipos de retorno, como no caso de OPTIONS para configurações CORS. A seguir:

```json
{
 "passthroughBehavior": "when_no_match",
  "connectionType": "MOCK",
  "timeoutMillis": 14000,
  "type": "http_proxy",
  "httpMethod": "OPTIONS",
  "requestTemplates": {
    "application/json": "{\"statusCode\": 200}"
  },
  "responses": {
    "default":{
      "statusCode": "200",
      "responseParameters": {
        "method.response.header.Access-Control-Allow-Headers": "'Access-Control-Allow-Origin,Content-Type,X-Amz,Data,Authorization,X-Api-Key'",
        "method.response.header.Access-Control-Allow-Methods": "'*'",
        "method.response.header.Access-Control-Allow-Origin": "'*'"
      },
      "responseTemplates": {
        "application/json": "{}"
      }
    }
  }
}
```
No exemplo acima, é criado um método OPTIONS que irá retornar os headers Access-Control-Allow-Headers(com os valores informados) e os demais Access-Control-Allow-Methods e Access-Control-Allow-Origin com valor '*', desta forma requisições de pre-flight CORS irão ter resposta válida, desde que a configuração de rota esteja feita corretamente.

Ao finalizr, o arquivo deve ser armazenado como, ex :**options-rota.json** no diretório de backend(AWS/Backends/backend-{DEV,HOM,PROD}).

### 3.7 Private Integration

O tipo de integração private é utilizado quando se expõe recursos dentro de uma VPC para serem acessados por clientes de fora dela. Neste caso utiliza-se um Network Load Balancer e um VPC Link. No NLB são configurados os listeners e target groups. Para realizar esta configuração, utiliza-se a URL do NLB no parâmetro "**uri**" e o type pode ser tanto **http** quanto **http_proxy**, ao depender da especificação para utilização, se deseja-se tratar a integration response e request, deve-se utilizar o **http** e para caso de repasse da requisição ao backend e retorno direto ao cliente, utiliza-se a **http_proxy**.

```json
{
  "passthroughBehavior": "when_no_match",
  "connectionType": "VPC_LINK",
  //pode-se passar a string ou utilizar a variável de stage com o valor da VPC link preenchido.
  "connectionId": "9h5iih",
  "timeoutMillis": 14000,
  "uri": "https://nlb-net-teste-121212121212.elb.sa-east-1.amazonaws.com:443/api/v1/teste",
  "type": "http_proxy",
  "httpMethod": "GET",
  "responses": {
    "default":{
      "statusCode": "200"
    },
    "201": {
      "statusCode": "201"
    }
  },
  "requestParameters": {
    "integration.request.queryString.stage": "method.request.queryString.version",
    "integration.request.header.x-userid": "method.request.header.x-user-id",
    "integration.request.path.op": "method.request.path.service"
  }
}
```

Ao finalizr, o arquivo deve ser armazenado como, ex :**get-teste.json** no diretório de backend(AWS/Backends/backend-{DEV,HOM,PROD}).
