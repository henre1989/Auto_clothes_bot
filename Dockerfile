FROM python:3
WORKDIR /home/autobot
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY Yekaterinburg /etc/localtime
COPY d_s.py ./
COPY bot.py ./
CMD [ "python", "./bot.py" ]