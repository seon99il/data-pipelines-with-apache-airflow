FROM python:3.8-slim

RUN pip install click pandas

COPY scripts/rank_movies.py /usr/local/bin/rank-movies
RUN chmod +x /usr/local/bin/rank-movies

ENV PATH="/usr/local/bin:${PATH}"