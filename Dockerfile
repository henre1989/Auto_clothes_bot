FROM python:3
WORKDIR /home/autobot
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY Moscow /etc/localtime
COPY d_s.py ./
COPY bot.py ./
RUN apt-get update && \
    apt-get install -y locales && \
    sed -i -e 's/# ru_RU.UTF-8 UTF-8/ru_RU.UTF-8 UTF-8/' /etc/locale.gen && \
    dpkg-reconfigure --frontend=noninteractive locales
ENV LC_ALL ru_RU.UTF-8
CMD [ "python", "./bot.py" ]