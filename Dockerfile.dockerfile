# Użyj oficjalnego obrazu Pythona jako podstawy
FROM python:3.9-slim-buster

# Ustaw katalog roboczy w kontenerze
WORKDIR /app

# Kopiuj plik requirements.txt do katalogu roboczego
COPY requirements.txt .

# Zainstaluj wszystkie zależności
RUN pip install --no-cache-dir -r requirements.txt

# Kopiuj resztę aplikacji do katalogu roboczego
COPY . .

# Ustaw zmienną środowiskową dla klucza API AISStream
# Zalecane jest użycie Cloud Secret Manager w produkcji, ale dla prostoty użyjemy zmiennej środowiskowej
ENV AISSTREAM_API_KEY="" # To zostanie nadpisane podczas deploymentu Cloud Run

# Komenda do uruchomienia aplikacji
CMD ["python", "main.py"]