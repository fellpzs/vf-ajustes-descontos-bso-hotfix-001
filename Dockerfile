FROM public.ecr.aws/d9s7b4j2/python:3.10-alpine

RUN useradd -ms /bin/bash reprocessador

USER reprocessador

WORKDIR /home/reprocessador/app/src

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY src/appReprocessaDescontos.py ./
COPY src/process_query.py ./

CMD [ "python", "./appReprocessaDescontos.py"]

