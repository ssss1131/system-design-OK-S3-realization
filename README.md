# OK-S3 Realization

Реализация S3-совместимого объектного хранилища на базе Cassandra и локальной файловой системы, с Flask-сервером и примерами на Python (boto3).

---

## Описание

Этот проект представляет собой мини-реализацию S3-совместимого объектного хранилища на Python + Flask, с хранением:

- **Метаданных** и ссылок на блоки в Cassandra (keyspace `ok_s3`)  
- **Данных** (файлов-блоков) на диске в каталоге `obs_data/`  
- **FSM-статуса** блоков, **lock-таблицы**, и **иерархических таблиц** для управления «папками»

В комплекте — простые клиентские скрипты на `boto3` для загрузки (multipart) и удаления объектов.

---

## Возможности

- Инициация multipart-загрузки (`InitiateMultipartUpload`)  
- Загрузка частей (`upload_part`)  
- Завершение загрузки (`CompleteMultipartUpload`)
- Дедупликация одинаковых обьектов  
- Листинг «папки» (`GET /files/{bucket}/{prefix}`)  
- Скачивание собранного объекта (`GET /{bucket}/{prefix}/{name}`)  
- Удаление объекта (`DELETE /{bucket}/{prefix}/{name}`)  

---

## Требования

- Python 3.8+ - 3.12-
- Docker & Docker Compose  
- Пакеты из `requirements.txt` (Flask, cassandra-driver, boto3 и др.)  

---

## Установка

### 1. Клонирование репозитория

```bash
git clone https://github.com/ssss1131/system-design-OK-S3-realization.git
cd system-design-OK-S3-realization
```
### 2. Скачать библиотеки
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```
### 3. Запустить докер контейнер
```bash
docker-compose up -d
```

### 4. Запустить сервер
``` bash
python server.py
```
### 5. Запустить тестовые 
``` bash
python upload_test.py
```
Должно выйти OK если все прошло успешно и книга загрузилась

можете перейти в браузер и написать 
http://localhost:5000/b/a/d/book.pdf
тогда сможете скачать загруженную книгу

и еще можете попробовать загрузить тот же файл с другим путем и наглядно увидеть дедупликацию, в папке obs_data новые блоки не появятся
```bash
python delete_test.py
```
а этот скрипт удаляет файл
