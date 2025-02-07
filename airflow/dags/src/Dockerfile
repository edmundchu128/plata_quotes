FROM python:3.9.20

# install packages
COPY requirements.txt .

RUN pip install -r requirements.txt

COPY . .

CMD ["python", "ingest_file.py"]