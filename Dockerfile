# Используйте базовый образ Python
FROM python:3.12-slim

# Установите рабочую директорию
WORKDIR /app

# Скопируйте requirements.txt и установите зависимости
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Скопируйте код бота
COPY main.py .

# Запустите бот
CMD ["python", "main.py"]
