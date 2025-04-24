# Integracja Apache Kafka i Apache Flink

Ten projekt demonstruje integrację Apache Kafka i Apache Flink do przetwarzania danych strumieniowych.

## Wymagania

- Docker i Docker Compose
- Python 3.7+
- Biblioteki Python: kafka-python, matplotlib, pyflink

## Instalacja

1. Uruchom skrypt instalacyjny, aby zainstalować wymagane zależności:

```bash
./scripts/setup.sh
```

## Zatrzymanie

Powinno działać klasyczne ctrl + C ale czasem nie działa wtedy należy dodać sobie

``` bash
pkill -f "python.*temperature_generator.py" 
```

dalej w debugu
