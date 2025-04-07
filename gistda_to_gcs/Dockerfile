FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt ./
RUN pip install -r requirements.txt
COPY . .
ENV PORT=8080
ENV FUNCTION_TARGET=main
CMD ["functions-framework", "--target=main"]