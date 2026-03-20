FROM apify/actor-python:3.11

COPY requirements.txt ./
RUN pip install -r requirements.txt --no-cache-dir

COPY . ./

CMD ["python", "-m", "src"]