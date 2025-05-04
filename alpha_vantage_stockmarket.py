import requests
import json

API_KEY = "4H29A6N1IXR30ZSJ"
FUNCTION = "TIME_SERIES_DAILY"
SYMBOL = "PETR4.SAO"
OUTPUTSIZE = "compact"

# Monta a URL
url = f"https://www.alphavantage.co/query?function={FUNCTION}&symbol={SYMBOL}&apikey={API_KEY}&outputsize={OUTPUTSIZE}"

try:
    response = requests.get(url, timeout=10)
    data = response.json()

    # Verifica se há erro
    if "Error Message" in data:
        print("Erro na requisição:", data["Error Message"])
    elif "Time Series (Daily)" in data:
        print(f"Dados recebidos para {SYMBOL}. Exemplo de uma data:")
        # Mostra um exemplo de data e valores
        for date, values in list(data["Time Series (Daily)"].items())[:1]:
            print(f"Data: {date}")
            print(json.dumps(values, indent=2))
    else:
        print("Resposta inesperada da API:")
        print(json.dumps(data, indent=2))

except requests.exceptions.RequestException as e:
    print(f"Erro na requisição: {e}")
