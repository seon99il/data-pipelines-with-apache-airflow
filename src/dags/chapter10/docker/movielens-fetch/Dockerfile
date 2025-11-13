FROM python:3.8-slim


RUN python -m pip install click==7.1.1 requests==2.23.0

COPY scripts/fetch_ratings.py /usr/local/bin/fetch-ratings
RUN chmod +x /usr/local/bin/fetch-ratings

# 컨테이너 내에서 fetch-ratings 명령어를 사용할 수 있도록 PATH 설정
ENV PATH="/usr/local/bin:${PATH}"