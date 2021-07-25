FROM python:3.8-slim-buster

ENV group_id 0

WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

COPY . .

CMD ["python3", "main.py", "--group_id=${group_id}"]