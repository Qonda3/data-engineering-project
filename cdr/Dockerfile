FROM python:3.10-slim
WORKDIR /app
COPY . .
RUN pip install --no-cache-dir -r requirements.txt
# copy the wait script and ensure it's executable
COPY wait-for-sftp.sh /app/wait-for-sftp.sh
RUN chmod +x /app/wait-for-sftp.sh

# Use the wait script as entrypoint so the container will wait for SFTP to be reachable
ENTRYPOINT ["/app/wait-for-sftp.sh"]
CMD ["python", "main.py"]
