import pandas as pd
import requests
import json

# --- Configurações que você PRECISA AJUSTAR ---
# 1. Caminho para o seu arquivo CSV na VM Ubuntu
#    
csv_file_path = '/media/sf_archive/animal_disease_dataset.csv'

# 2. URL do PostgREST no seu PC Windows (Host)
#    
#    - A porta padrão do PostgREST é 3000.
postgrest_url = 'http://192.168.0.14:3000/animal_diseases' # Certifique-se que o IP está correto!

# --- Função para ler CSV e enviar para PostgREST ---
def send_csv_to_postgrest(csv_path, url):
    try:
        # Lê o arquivo CSV para um DataFrame do Pandas
        df = pd.read_csv(csv_path)

        # Converte cada linha do DataFrame para um dicionário Python.
        # O PostgREST espera um array de objetos JSON para inserção em massa.
        data_to_send = df.to_dict(orient='records') # 'records' transforma cada linha em um dicionário

        # Define os cabeçalhos da requisição HTTP
        headers = {
            'Content-Type': 'application/json', # Indica que estamos enviando JSON
            'Prefer': 'return=representation'   # Pede que o PostgREST retorne os dados inseridos
        }

        print(f"Iniciando envio de {len(data_to_send)} linhas do CSV para a URL: {url}")

        # Envia a requisição POST com os dados JSON
        response = requests.post(url, headers=headers, data=json.dumps(data_to_send))

        # Verifica se a requisição foi bem-sucedida (status code 2xx)
        response.raise_for_status()

        print("Dados enviados com sucesso para o PostgREST!")
        print("Resposta do PostgREST:")
        # Imprime a resposta do PostgREST (geralmente os dados que foram inseridos)
        print(json.dumps(response.json(), indent=2))

    except FileNotFoundError:
        print(f"ERRO: O arquivo CSV '{csv_path}' não foi encontrado.")
        print("Verifique o caminho do arquivo e o nome.")
    except pd.errors.EmptyDataError:
        print(f"ERRO: O arquivo CSV '{csv_path}' está vazio ou não possui cabeçalhos.")
    except requests.exceptions.ConnectionError:
        print(f"ERRO DE CONEXÃO: Não foi possível conectar ao PostgREST em {url}.")
        print("1. Verifique se o PostgREST está rodando no seu Windows (o CMD está aberto e não tem erros).")
        print("2. Verifique se o Firewall do Windows permite a porta 3000 para o PostgREST.")
        print("3. Certifique-se de que o IP do host no script Python (SEU_IP_DO_HOST) está correto.")
        print("4. Verifique a configuração de rede da sua VM (ping o IP do host).")
    except requests.exceptions.RequestException as e:
        print(f"ERRO NA REQUISIÇÃO HTTP: {e}")
        if 'response' in locals() and response is not None:
            print(f"Status Code da Resposta: {response.status_code}")
            print(f"Corpo da Resposta do Servidor: {response.text}")
            print("Verifique se o nome da tabela na URL do PostgREST está correto e se as colunas correspondem.")
    except json.JSONDecodeError:
        print("ERRO: Resposta do PostgREST não é um JSON válido. Verifique os logs do PostgREST.")
    except Exception as e:
        print(f"OCORREU UM ERRO INESPERADO: {e}")

# Executa a função principal quando o script é rodado
if __name__ == "__main__":
    send_csv_to_postgrest(csv_file_path, postgrest_url)