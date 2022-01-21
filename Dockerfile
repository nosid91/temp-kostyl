FROM python:3.10
WORKDIR /service
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . ./
ENTRYPOINT ["python3", "main.py"]
