FROM python:3.8
RUN pip3 install requests
RUN pip3 install snowflake-connector-python
RUN pip3 install slack_webhook
RUN pip3 install google-api-python-client
RUN pip3 install oauth2client
RUN pip3 install google-cloud-storage
COPY . .
CMD ["python3","new_code.py", "AMEX"]